package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
)

// Config describes a copy operation.
type Config struct {
	Sources        []string
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
	Stats          stats.ReadWriter  // nil = engine creates its own
	Filter         *filter.Chain     // nil = no filtering
	Delete         bool              // delete extraneous files from destination
	Verify         bool              // post-copy BLAKE3 checksum verification
	NoTimes        bool              // disable mtime preservation (disables skip detection)
	WorkerLimit    *atomic.Int32     // runtime worker throttle; nil = no throttling
	SrcEndpoint    transport.ReadEndpoint  // nil = local (created from Sources)
	DstEndpoint    transport.WriteEndpoint // nil = local (created from Dst)
	Delta          bool                    // use delta transfer for remote copies
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

// resolvedSource represents a single source after resolving trailing-slash
// semantics and computing the destination base path.
type resolvedSource struct {
	srcPath      string      // absolute, cleaned path
	dstBase      string      // computed destination for this source
	isFile       bool
	copyContents bool // trailing slash on dir = copy contents, not dir itself
	info         os.FileInfo
}

// resolveSources resolves each raw source argument into a resolvedSource,
// applying rsync-compatible trailing-slash semantics:
//   - dir/  → copy contents of dir into dst
//   - dir   → copy dir itself into dst (creates dst/dir/)
//   - file  → copy file into dst
func resolveSources(sources []string, dst string, recursive bool) ([]resolvedSource, error) {
	resolved := make([]resolvedSource, 0, len(sources))

	for _, raw := range sources {
		// Detect trailing slash before filepath.Clean strips it.
		hasTrailingSlash := strings.HasSuffix(raw, string(filepath.Separator)) ||
			strings.HasSuffix(raw, "/")

		cleaned := filepath.Clean(raw)
		info, err := os.Lstat(cleaned)
		if err != nil {
			return nil, fmt.Errorf("source: %w", err)
		}

		rs := resolvedSource{
			srcPath: cleaned,
			info:    info,
			isFile:  !info.IsDir(),
		}

		if info.IsDir() {
			if !recursive {
				return nil, fmt.Errorf("source %s is a directory (use -r or -a)", cleaned)
			}
			if hasTrailingSlash {
				// dir/ → copy contents into dst directly
				rs.copyContents = true
				rs.dstBase = dst
			} else {
				// dir → copy dir itself into dst/basename
				rs.dstBase = filepath.Join(dst, filepath.Base(cleaned))
			}
		} else {
			// File source: destination is dst/basename (if dst is a dir),
			// or dst itself (handled later for single-file case).
			rs.dstBase = filepath.Join(dst, filepath.Base(cleaned))
		}

		resolved = append(resolved, rs)
	}

	return resolved, nil
}

// Run executes a copy operation, blocking until complete.
func Run(ctx context.Context, cfg Config) Result {
	if len(cfg.Sources) == 0 {
		return Result{Err: fmt.Errorf("no sources specified")}
	}

	// Archive implies recursive + all preserve flags.
	recursive := cfg.Recursive || cfg.Archive

	resolved, err := resolveSources(cfg.Sources, cfg.Dst, recursive)
	if err != nil {
		return Result{Err: err}
	}

	collector := cfg.Stats
	if collector == nil {
		collector = stats.NewCollector()
	}

	// Single file source with no other sources: use optimized file copy path.
	if len(resolved) == 1 && resolved[0].isFile {
		return runFileCopy(ctx, cfg, collector, resolved[0])
	}

	// Multi-source to non-directory destination check: if dst exists and is a
	// file, that's an error when we have multiple sources or directory sources.
	if len(resolved) > 1 {
		if dstInfo, err := os.Stat(cfg.Dst); err == nil && !dstInfo.IsDir() {
			return Result{Err: fmt.Errorf("destination %s is not a directory (multiple sources require a directory destination)", cfg.Dst)}
		}
	}

	return runMultiSourceCopy(ctx, cfg, collector, resolved)
}

// ensureSrcEndpoint returns the configured endpoint or creates a local one.
func ensureSrcEndpoint(ep transport.ReadEndpoint, root string) transport.ReadEndpoint {
	if ep != nil {
		return ep
	}
	return transport.NewLocalReadEndpoint(root)
}

// ensureDstEndpoint returns the configured endpoint or creates a local one.
func ensureDstEndpoint(ep transport.WriteEndpoint, root string) transport.WriteEndpoint {
	if ep != nil {
		return ep
	}
	return transport.NewLocalWriteEndpoint(root)
}

func runMultiSourceCopy(ctx context.Context, cfg Config, collector stats.ReadWriter, sources []resolvedSource) Result {
	var copyErr error

	// Prescan all directory sources for progress totals.
	var totalFiles, totalBytes int64
	for _, rs := range sources {
		if rs.isFile {
			totalFiles++
			totalBytes += rs.info.Size()
		} else {
			f, b := Prescan(ctx, rs.srcPath, cfg.Filter)
			totalFiles += f
			totalBytes += b
		}
	}
	collector.SetTotals(totalFiles, totalBytes)
	cfg.emit(event.Event{
		Type:      event.ScanComplete,
		Total:     totalFiles,
		TotalSize: totalBytes,
	})

	// Worker pool endpoints: SrcEndpoint rooted at "/" so filepath.Rel works
	// for any absolute source path. DstEndpoint rooted at cfg.Dst.
	workerSrcEP := ensureSrcEndpoint(cfg.SrcEndpoint, "/")
	workerDstEP := ensureDstEndpoint(cfg.DstEndpoint, cfg.Dst)

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
		SrcEndpoint:   workerSrcEP,
		DstEndpoint:   workerDstEP,
		Delta:         cfg.Delta,
	}

	wp, err := NewWorkerPool(workerCfg)
	if err != nil {
		return Result{Err: fmt.Errorf("create worker pool: %w", err)}
	}
	defer wp.Close()

	// Merged task channel: all sources feed into one channel consumed by the
	// shared worker pool. Directory sources are scanned sequentially but the
	// workers stay busy processing tasks from the previous source.
	allTasks := make(chan FileTask, cfg.Workers*2)
	allErrs := make(chan error, 64)

	var feedWg sync.WaitGroup
	feedWg.Add(1)
	go func() {
		defer feedWg.Done()
		defer close(allTasks)

		for _, rs := range sources {
			if ctx.Err() != nil {
				return
			}

			if rs.isFile {
				// File source: emit a single task directly.
				task, err := fileInfoToTask(rs.srcPath, rs.dstBase, rs.info)
				if err != nil {
					select {
					case allErrs <- err:
					default:
					}
					continue
				}
				select {
				case allTasks <- task:
				case <-ctx.Done():
					return
				}
				continue
			}

			// Directory source: ensure destination exists and run scanner.
			if err := os.MkdirAll(rs.dstBase, 0755); err != nil {
				select {
				case allErrs <- fmt.Errorf("create destination %s: %w", rs.dstBase, err):
				default:
				}
				continue
			}

			// Per-source endpoints: rooted at the source/dest pair so
			// relative paths in scanner/delete/verify match the endpoint root.
			srcEP := ensureSrcEndpoint(cfg.SrcEndpoint, rs.srcPath)
			dstEP := ensureDstEndpoint(cfg.DstEndpoint, rs.dstBase)

			scanCfg := ScannerConfig{
				SrcRoot:        rs.srcPath,
				DstRoot:        rs.dstBase,
				Workers:        cfg.ScanWorkers,
				ChunkThreshold: cfg.ChunkThreshold,
				SparseDetect:   true,
				IncludeXattrs:  cfg.Archive,
				Events:         cfg.Events,
				Filter:         cfg.Filter,
				Stats:          collector,
				SrcEndpoint:    srcEP,
				DstEndpoint:    dstEP,
			}

			scanner := NewScanner(scanCfg)
			tasks, scanErrs := scanner.Scan(ctx)

			// Drain scanner errors in background.
			var scanErrWg sync.WaitGroup
			scanErrWg.Add(1)
			go func() {
				defer scanErrWg.Done()
				for err := range scanErrs {
					select {
					case allErrs <- err:
					default:
					}
				}
			}()

			// Drain tasks into the merged channel.
			for task := range tasks {
				select {
				case allTasks <- task:
				case <-ctx.Done():
					scanErrWg.Wait()
					return
				}
			}
			scanErrWg.Wait()
		}
	}()

	// Run workers (blocks until allTasks is closed and all tasks processed).
	wp.Run(ctx, allTasks, allErrs)

	// Wait for the feed goroutine (should already be done since allTasks is closed).
	feedWg.Wait()

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

	// Delete extraneous files from destination (per source-dst pair).
	if cfg.Delete {
		// Detect overlapping destinations: if multiple sources map to the same
		// dstBase, skip delete with a warning to avoid incorrect deletions.
		dstCounts := make(map[string]int)
		for _, rs := range sources {
			if !rs.isFile {
				dstCounts[rs.dstBase]++
			}
		}

		for _, rs := range sources {
			if rs.isFile {
				continue
			}
			if dstCounts[rs.dstBase] > 1 {
				slog.Warn("skipping --delete for overlapping destination",
					"src", rs.srcPath, "dst", rs.dstBase)
				continue
			}
			delCfg := DeleteConfig{
				SrcRoot:     rs.srcPath,
				DstRoot:     rs.dstBase,
				Filter:      cfg.Filter,
				DryRun:      cfg.DryRun,
				Events:      cfg.Events,
				SrcEndpoint: ensureSrcEndpoint(cfg.SrcEndpoint, rs.srcPath),
				DstEndpoint: ensureDstEndpoint(cfg.DstEndpoint, rs.dstBase),
			}
			if _, err := DeleteExtraneous(ctx, delCfg); err != nil && copyErr == nil {
				copyErr = err
			}
		}
	}

	// Post-copy verification (per source-dst pair).
	if cfg.Verify && !cfg.DryRun {
		for _, rs := range sources {
			if rs.isFile {
				continue
			}
			vr := Verify(ctx, VerifyConfig{
				SrcRoot:     rs.srcPath,
				DstRoot:     rs.dstBase,
				Workers:     cfg.Workers,
				Filter:      cfg.Filter,
				Events:      cfg.Events,
				Stats:       collector,
				SrcEndpoint: ensureSrcEndpoint(cfg.SrcEndpoint, rs.srcPath),
				DstEndpoint: ensureDstEndpoint(cfg.DstEndpoint, rs.dstBase),
			})
			if vr.Failed > 0 && copyErr == nil {
				copyErr = fmt.Errorf("%d files failed verification", vr.Failed)
			}
		}
	}

	return Result{
		Stats: collector.Snapshot(),
		Err:   copyErr,
	}
}

func runFileCopy(ctx context.Context, cfg Config, collector stats.ReadWriter, rs resolvedSource) Result {
	dst := rs.dstBase

	// If dst is an existing directory, copy into it.
	if dstInfo, err := os.Stat(cfg.Dst); err == nil && dstInfo.IsDir() {
		dst = filepath.Join(cfg.Dst, filepath.Base(rs.srcPath))
	} else if len(cfg.Sources) == 1 {
		// Single file to non-existing or file destination: use dst directly.
		dst = cfg.Dst
	}

	dstDir := filepath.Dir(dst)

	// Ensure parent directory exists.
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return Result{Err: fmt.Errorf("create parent dir: %w", err)}
	}

	srcEP := ensureSrcEndpoint(cfg.SrcEndpoint, filepath.Dir(rs.srcPath))
	dstEP := ensureDstEndpoint(cfg.DstEndpoint, dstDir)

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
		DstRoot:       dstDir,
		SrcEndpoint:   srcEP,
		DstEndpoint:   dstEP,
		Delta:         cfg.Delta,
	}

	wp, err := NewWorkerPool(workerCfg)
	if err != nil {
		return Result{Err: fmt.Errorf("create worker pool: %w", err)}
	}
	defer wp.Close()

	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)

	task, err := fileInfoToTask(rs.srcPath, dst, rs.info)
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
