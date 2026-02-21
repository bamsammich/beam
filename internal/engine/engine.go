package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
)

// Config describes a copy operation.
type Config struct {
	Src            string
	Dst            string
	Recursive      bool
	Archive        bool
	Workers        int
	ScanWorkers    int
	ChunkThreshold int64
	DryRun         bool
	Verbose        bool
	Quiet          bool
	UseIOURing     bool
	Events         chan<- event.Event // nil = no events emitted
	Stats          *stats.Collector  // nil = engine creates its own
	Filter         *filter.Chain     // nil = no filtering
	Delete         bool              // delete extraneous files from destination
	Verify         bool              // post-copy BLAKE3 checksum verification
	NoTimes        bool              // disable mtime preservation (disables skip detection)
	WorkerLimit    *atomic.Int32     // runtime worker throttle; nil = no throttling
}

func (c Config) emit(e event.Event) {
	if c.Events == nil {
		return
	}
	e.Timestamp = time.Now()
	select {
	case c.Events <- e:
	default: // never block engine
	}
}

// Result is the outcome of a copy operation.
type Result struct {
	Stats stats.Snapshot
	Err   error
}

// Run executes a copy operation, blocking until complete.
func Run(ctx context.Context, cfg Config) Result {
	// Validate inputs.
	srcInfo, err := os.Lstat(cfg.Src)
	if err != nil {
		return Result{Err: fmt.Errorf("source: %w", err)}
	}

	// Archive implies recursive + all preserve flags.
	recursive := cfg.Recursive || cfg.Archive

	if srcInfo.IsDir() && !recursive {
		return Result{Err: fmt.Errorf("source %s is a directory (use -r or -a)", cfg.Src)}
	}

	collector := cfg.Stats
	if collector == nil {
		collector = stats.NewCollector()
	}

	if srcInfo.IsDir() {
		return runDirCopy(ctx, cfg, collector, recursive)
	}
	return runFileCopy(ctx, cfg, collector, srcInfo)
}

func runDirCopy(ctx context.Context, cfg Config, collector *stats.Collector, recursive bool) Result {
	// Ensure destination root exists.
	if err := os.MkdirAll(cfg.Dst, 0755); err != nil {
		return Result{Err: fmt.Errorf("create destination: %w", err)}
	}

	var copyErr error

	// Prescan: quickly count files and bytes for accurate progress display.
	// This only does readdir+lstat (no file opens), so it's very fast.
	totalFiles, totalBytes := Prescan(ctx, cfg.Src, cfg.Filter)
	collector.SetTotals(totalFiles, totalBytes)
	cfg.emit(event.Event{
		Type:      event.ScanComplete,
		Total:     totalFiles,
		TotalSize: totalBytes,
	})

	scanCfg := ScannerConfig{
		SrcRoot:        cfg.Src,
		DstRoot:        cfg.Dst,
		Workers:        cfg.ScanWorkers,
		ChunkThreshold: cfg.ChunkThreshold,
		SparseDetect:   true,
		IncludeXattrs:  cfg.Archive,
		Events:         cfg.Events,
		Filter:         cfg.Filter,
		Stats:          collector,
	}

	workerCfg := WorkerConfig{
		NumWorkers:    cfg.Workers,
		PreserveMode:  cfg.Archive,
		PreserveTimes: cfg.Archive && !cfg.NoTimes,
		PreserveOwner: cfg.Archive,
		PreserveXattr: cfg.Archive,
		NoTimes:       cfg.NoTimes,
		DryRun:        cfg.DryRun,
		UseIOURing:    cfg.UseIOURing,
		Stats:         collector,
		Events:        cfg.Events,
		DstRoot:       cfg.Dst,
		WorkerLimit:   cfg.WorkerLimit,
	}

	scanner := NewScanner(scanCfg)
	tasks, scanErrs := scanner.Scan(ctx)

	wp, err := NewWorkerPool(workerCfg)
	if err != nil {
		return Result{Err: fmt.Errorf("create worker pool: %w", err)}
	}
	defer wp.Close()

	// Collect errors from both scanner and workers.
	allErrs := make(chan error, 64)

	// Drain scanner errors in a goroutine.
	go func() {
		for err := range scanErrs {
			select {
			case allErrs <- err:
			default:
			}
		}
	}()

	// Run workers (blocks until all tasks processed).
	wp.Run(ctx, tasks, allErrs)

	// Close and drain error channel.
	close(allErrs)
	var errCount int
	for err := range allErrs {
		errCount++
		if copyErr == nil {
			copyErr = err
		}
	}

	if errCount > 1 {
		copyErr = fmt.Errorf("%w (and %d more errors)", copyErr, errCount-1)
	}

	// Delete extraneous files from destination.
	if cfg.Delete {
		delCfg := DeleteConfig{
			SrcRoot: cfg.Src,
			DstRoot: cfg.Dst,
			Filter:  cfg.Filter,
			DryRun:  cfg.DryRun,
			Events:  cfg.Events,
		}
		if _, err := DeleteExtraneous(ctx, delCfg); err != nil && copyErr == nil {
			copyErr = err
		}
	}

	// Post-copy verification.
	if cfg.Verify && !cfg.DryRun {
		vr := Verify(ctx, VerifyConfig{
			SrcRoot: cfg.Src,
			DstRoot: cfg.Dst,
			Workers: cfg.Workers,
			Filter:  cfg.Filter,
			Events:  cfg.Events,
			Stats:   collector,
		})
		if vr.Failed > 0 && copyErr == nil {
			copyErr = fmt.Errorf("%d files failed verification", vr.Failed)
		}
	}

	return Result{
		Stats: collector.Snapshot(),
		Err:   copyErr,
	}
}

func runFileCopy(ctx context.Context, cfg Config, collector *stats.Collector, srcInfo os.FileInfo) Result {
	dst := cfg.Dst

	// If dst is an existing directory, copy into it.
	if dstInfo, err := os.Stat(dst); err == nil && dstInfo.IsDir() {
		dst = filepath.Join(dst, filepath.Base(cfg.Src))
	}

	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return Result{Err: fmt.Errorf("create parent dir: %w", err)}
	}

	workerCfg := WorkerConfig{
		NumWorkers:    1,
		PreserveMode:  cfg.Archive,
		PreserveTimes: cfg.Archive && !cfg.NoTimes,
		PreserveOwner: cfg.Archive,
		PreserveXattr: cfg.Archive,
		NoTimes:       cfg.NoTimes,
		DryRun:        cfg.DryRun,
		UseIOURing:    cfg.UseIOURing,
		Stats:         collector,
	}

	wp, err := NewWorkerPool(workerCfg)
	if err != nil {
		return Result{Err: fmt.Errorf("create worker pool: %w", err)}
	}
	defer wp.Close()

	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)

	task, err := fileInfoToTask(cfg.Src, dst, srcInfo)
	if err != nil {
		return Result{Err: err}
	}

	tasks <- task
	close(tasks)

	wp.Run(ctx, tasks, errs)
	close(errs)

	var copyErr error
	for err := range errs {
		copyErr = err
	}

	return Result{
		Stats: collector.Snapshot(),
		Err:   copyErr,
	}
}

func fileInfoToTask(srcPath, dstPath string, info os.FileInfo) (FileTask, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return FileTask{}, fmt.Errorf("unsupported stat type for %s", srcPath)
	}

	task := FileTask{
		SrcPath: srcPath,
		DstPath: dstPath,
		Type:    Regular,
		Size:    info.Size(),
		Mode:    uint32(info.Mode()),
		Uid:     stat.Uid,
		Gid:     stat.Gid,
		ModTime: info.ModTime(),
		AccTime: atimeFromStat(stat),
	}

	// Detect sparse segments.
	if info.Size() > 0 {
		fd, err := os.Open(srcPath)
		if err != nil {
			return task, nil // proceed without sparse detection
		}
		segments, err := DetectSparseSegments(fd, info.Size())
		fd.Close()
		if err == nil {
			task.Segments = segments
		}
	}

	return task, nil
}
