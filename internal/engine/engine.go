package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
)

// Config describes a copy operation.
type Config struct {
	Stats          stats.ReadWriter
	SrcTransport   transport.Transport
	DstTransport   transport.Transport
	Events         chan<- event.Event
	Filter         *filter.Chain
	WorkerLimit    *atomic.Int32
	Dst            string
	Sources        []string
	ScanWorkers    int
	Workers        int
	ChunkThreshold int64
	Recursive      bool
	Archive        bool
	DryRun         bool
	Verbose        bool
	Quiet          bool
	UseIOURing     bool
	Delete         bool
	Verify         bool
	NoTimes        bool
	Delta          bool
	BWLimit        int64 // bytes per second; 0 = unlimited
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
	Err   error
	Stats stats.Snapshot
}

// resolvedSource represents a single source after resolving trailing-slash
// semantics and computing the destination base path.
type resolvedSource struct {
	srcPath      string // absolute, cleaned path
	dstBase      string // computed destination for this source
	entry        transport.FileEntry
	isFile       bool
	copyContents bool // trailing slash on dir = copy contents, not dir itself
}

// resolveSources resolves each raw source argument into a resolvedSource,
// applying rsync-compatible trailing-slash semantics:
//   - dir/  → copy contents of dir into dst
//   - dir   → copy dir itself into dst (creates dst/dir/)
//   - file  → copy file into dst
//
// When srcEP is non-nil (remote source), it is used to stat the source
// instead of os.Lstat, which would fail for paths that only exist on the
// remote machine.
//
//nolint:revive // cognitive-complexity: rsync-compatible source resolution with trailing-slash semantics
func resolveSources(
	sources []string,
	dst string,
	recursive bool,
	srcEP transport.Reader,
) ([]resolvedSource, error) {
	resolved := make([]resolvedSource, 0, len(sources))

	for _, raw := range sources {
		// Detect trailing slash before filepath.Clean strips it.
		hasTrailingSlash := strings.HasSuffix(raw, string(filepath.Separator)) ||
			strings.HasSuffix(raw, "/")

		cleaned := filepath.Clean(raw)

		var entry transport.FileEntry
		if srcEP != nil {
			// Remote source: stat via endpoint ("." = endpoint root = source path).
			var err error
			entry, err = srcEP.Stat(".")
			if err != nil {
				return nil, fmt.Errorf("source: %w", err)
			}
		} else {
			// Local source: stat via filesystem.
			info, err := os.Lstat(cleaned)
			if err != nil {
				return nil, fmt.Errorf("source: %w", err)
			}
			entry = transport.FileEntry{
				Size:      info.Size(),
				Mode:      info.Mode(),
				ModTime:   info.ModTime(),
				IsDir:     info.IsDir(),
				IsSymlink: info.Mode()&os.ModeSymlink != 0,
			}
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				entry.UID = stat.Uid
				entry.GID = stat.Gid
				entry.AccTime = atimeFromStat(stat)
				entry.Nlink = uint32(stat.Nlink) //nolint:gosec // G115: nlink fits in uint32
				entry.Ino = stat.Ino
				entry.Dev = devFromStat(stat)
			}
		}

		rs := resolvedSource{
			srcPath: cleaned,
			entry:   entry,
			isFile:  !entry.IsDir,
		}

		if entry.IsDir {
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

// bwLimiter returns a shared rate.Limiter for the given Config, or nil if no
// bandwidth limit is configured.
func bwLimiter(cfg Config) *rate.Limiter {
	if cfg.BWLimit <= 0 {
		return nil
	}
	return NewBWLimiter(cfg.BWLimit)
}

// Run executes a copy operation, blocking until complete.
//
//nolint:revive // cognitive-complexity: top-level orchestrator with necessary branching
func Run(ctx context.Context, cfg Config) Result {
	if len(cfg.Sources) == 0 {
		return Result{Err: errors.New("no sources specified")}
	}

	if cfg.SrcTransport == nil {
		return Result{Err: errors.New("SrcTransport is required")}
	}
	if cfg.DstTransport == nil {
		return Result{Err: errors.New("DstTransport is required")}
	}

	// Archive implies recursive + all preserve flags.
	recursive := cfg.Recursive || cfg.Archive

	// Connect source endpoint for remote sources (used for stat during resolve).
	var srcEP transport.Reader
	if cfg.SrcTransport.Protocol() != transport.ProtocolLocal {
		var err error
		srcEP, err = cfg.SrcTransport.ReaderAt(cfg.Sources[0])
		if err != nil {
			return Result{Err: fmt.Errorf("connect source: %w", err)}
		}
	}

	resolved, err := resolveSources(cfg.Sources, cfg.Dst, recursive, srcEP)
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
			return Result{
				Err: fmt.Errorf(
					"destination %s is not a directory (multiple sources require a directory destination)",
					cfg.Dst,
				),
			}
		}
	}

	return runMultiSourceCopy(ctx, cfg, collector, resolved)
}

//nolint:ireturn // factory returns interface by design
func readerAt(
	t transport.Transport,
	root string,
) (transport.Reader, error) {
	return t.ReaderAt(root)
}

//nolint:ireturn // factory returns interface by design
func readWriterAt(
	t transport.Transport,
	root string,
) (transport.ReadWriter, error) {
	return t.ReadWriterAt(root)
}

//nolint:gocyclo,revive // cyclomatic: top-level orchestrator — prescan, scan, worker dispatch, delete, verify
func runMultiSourceCopy(
	ctx context.Context,
	cfg Config,
	collector stats.ReadWriter,
	sources []resolvedSource,
) Result {
	var copyErr error

	isRemote := cfg.SrcTransport.Protocol() != transport.ProtocolLocal

	// Prescan all directory sources for progress totals.
	var totalFiles, totalBytes int64
	for _, rs := range sources {
		switch {
		case rs.isFile:
			totalFiles++
			totalBytes += rs.entry.Size
		case isRemote:
			// Remote source: use endpoint Walk for prescan instead of local Prescan.
			srcEP, err := readerAt(cfg.SrcTransport, rs.srcPath)
			if err != nil {
				slog.Warn("remote prescan connect failed", "error", err)
				break
			}
			if err := srcEP.Walk(func(e transport.FileEntry) error {
				if e.Mode.IsRegular() {
					if cfg.Filter != nil && !cfg.Filter.Match(e.RelPath, false, e.Size) {
						return nil
					}
					totalFiles++
					totalBytes += e.Size
				}
				return nil
			}); err != nil {
				slog.Warn("remote prescan walk failed", "error", err, "endpoint_root", srcEP.Root())
			}
		default:
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

	// Worker pool endpoints: SrcTransport rooted at "/" so filepath.Rel works
	// for any absolute source path. DstTransport rooted at cfg.Dst.
	workerSrcEP, err := readerAt(cfg.SrcTransport, "/")
	if err != nil {
		return Result{Err: fmt.Errorf("connect source endpoint: %w", err)}
	}
	workerDstEP, err := readWriterAt(cfg.DstTransport, cfg.Dst)
	if err != nil {
		return Result{Err: fmt.Errorf("connect dest endpoint: %w", err)}
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
		BWLimiter:     bwLimiter(cfg),
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
				task := fileEntryToTask(rs.srcPath, rs.dstBase, rs.entry)
				if cfg.SrcTransport.Protocol() == transport.ProtocolLocal && task.Size > 0 {
					fd, openErr := os.Open(rs.srcPath)
					if openErr == nil {
						segments, sErr := DetectSparseSegments(fd, task.Size)
						fd.Close()
						if sErr == nil {
							task.Segments = segments
						}
					}
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
			srcEP, epErr := readerAt(cfg.SrcTransport, rs.srcPath)
			if epErr != nil {
				select {
				case allErrs <- fmt.Errorf("connect source for %s: %w", rs.srcPath, epErr):
				default:
				}
				continue
			}
			dstEP, epErr := readWriterAt(cfg.DstTransport, rs.dstBase)
			if epErr != nil {
				select {
				case allErrs <- fmt.Errorf("connect dest for %s: %w", rs.dstBase, epErr):
				default:
				}
				continue
			}

			// Build destination index for remote endpoints (avoids per-file Stat RPCs).
			dstIndex := buildDstIndex(dstEP, rs.dstBase)

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
				DstIndex:       dstIndex,
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
			delSrcEP, delErr := readerAt(cfg.SrcTransport, rs.srcPath)
			if delErr != nil {
				if copyErr == nil {
					copyErr = delErr
				}
				continue
			}
			delDstEP, delErr := readWriterAt(cfg.DstTransport, rs.dstBase)
			if delErr != nil {
				if copyErr == nil {
					copyErr = delErr
				}
				continue
			}
			delCfg := DeleteConfig{
				SrcRoot:     rs.srcPath,
				DstRoot:     rs.dstBase,
				Filter:      cfg.Filter,
				DryRun:      cfg.DryRun,
				Events:      cfg.Events,
				SrcEndpoint: delSrcEP,
				DstEndpoint: delDstEP,
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
			vfySrcEP, vfyErr := readerAt(cfg.SrcTransport, rs.srcPath)
			if vfyErr != nil {
				if copyErr == nil {
					copyErr = vfyErr
				}
				continue
			}
			vfyDstEP, vfyErr := readWriterAt(cfg.DstTransport, rs.dstBase)
			if vfyErr != nil {
				if copyErr == nil {
					copyErr = vfyErr
				}
				continue
			}
			vr := Verify(ctx, VerifyConfig{
				SrcRoot:     rs.srcPath,
				DstRoot:     rs.dstBase,
				Workers:     cfg.Workers,
				Filter:      cfg.Filter,
				Events:      cfg.Events,
				Stats:       collector,
				SrcEndpoint: vfySrcEP,
				DstEndpoint: vfyDstEP,
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

//nolint:revive // cognitive-complexity: single-file copy path with endpoint setup
func runFileCopy(
	ctx context.Context,
	cfg Config,
	collector stats.ReadWriter,
	rs resolvedSource,
) Result {
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

	srcEP, err := readerAt(cfg.SrcTransport, filepath.Dir(rs.srcPath))
	if err != nil {
		return Result{Err: fmt.Errorf("connect source: %w", err)}
	}
	dstEP, err := readWriterAt(cfg.DstTransport, dstDir)
	if err != nil {
		return Result{Err: fmt.Errorf("connect dest: %w", err)}
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
		DstRoot:       dstDir,
		BWLimiter:     bwLimiter(cfg),
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

	task := fileEntryToTask(rs.srcPath, dst, rs.entry)
	if cfg.SrcTransport.Protocol() == transport.ProtocolLocal && task.Size > 0 {
		fd, openErr := os.Open(rs.srcPath)
		if openErr == nil {
			segments, sErr := DetectSparseSegments(fd, task.Size)
			fd.Close()
			if sErr == nil {
				task.Segments = segments
			}
		}
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

// buildDstIndex pre-walks the destination to build a lookup table for skip
// detection. For endpoints implementing SubtreeWalker, this replaces N per-file
// Stat RPCs with a single streaming Walk RPC. Returns nil for endpoints where
// per-file Stat is cheap (e.g. local).
func buildDstIndex(
	ep transport.ReadWriter,
	dstBase string,
) map[string]transport.FileEntry {
	walker, ok := ep.(transport.SubtreeWalker)
	if !ok {
		return nil // no subtree walk support: per-file Stat is fast enough
	}

	// Compute the subtree to walk relative to the endpoint root.
	subDir, err := filepath.Rel(ep.Root(), dstBase)
	if err != nil {
		slog.Debug("buildDstIndex: rel path failed",
			"root", ep.Root(), "dstBase", dstBase, "error", err)
		return nil
	}
	if subDir == "." {
		subDir = ""
	}

	index := make(map[string]transport.FileEntry)
	if err := walker.WalkSubtree(subDir, func(entry transport.FileEntry) error {
		index[entry.RelPath] = entry
		return nil
	}); err != nil {
		// Walk failed (e.g. destination doesn't exist yet) — fall back to per-file Stat.
		slog.Debug("buildDstIndex: walk failed, falling back to per-file stat", "error", err)
		return nil
	}

	slog.Debug("buildDstIndex", "entries", len(index), "subDir", subDir)
	return index
}

// fileEntryToTask creates a FileTask from a transport.FileEntry.
// Unlike fileInfoToTask, this does not perform sparse detection (no local fd
// for remote files) and does not depend on syscall.Stat_t.
func fileEntryToTask(srcPath, dstPath string, entry transport.FileEntry) FileTask {
	return FileTask{
		SrcPath: srcPath,
		DstPath: dstPath,
		Type:    Regular,
		Size:    entry.Size,
		Mode:    uint32(entry.Mode),
		UID:     entry.UID,
		GID:     entry.GID,
		ModTime: entry.ModTime,
		AccTime: entry.AccTime,
	}
}
