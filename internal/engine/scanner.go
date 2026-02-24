package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
)

// ScannerConfig controls scanner behavior.
type ScannerConfig struct {
	Events         chan<- event.Event
	Filter         *filter.Chain
	Stats          stats.Writer                   // if set, skipped-file stats are recorded directly
	SrcEndpoint    transport.Reader               // nil = local filesystem
	DstEndpoint    transport.ReadWriter           // nil = local filesystem
	DstIndex       map[string]transport.FileEntry // pre-populated destination index (avoids per-file Stat RPCs)
	SrcRoot        string
	DstRoot        string
	ChunkThreshold int64
	Workers        int
	FollowSymlinks bool
	IncludeXattrs  bool
	SparseDetect   bool
}

// Scanner traverses a directory tree in parallel and emits FileTask items.
type Scanner struct {
	tasks      chan FileTask
	errs       chan error
	inodeSeen  sync.Map
	cfg        ScannerConfig
	totalFiles atomic.Int64
	totalBytes atomic.Int64
}

// NewScanner creates a scanner with the given config.
// SrcEndpoint and DstEndpoint must be set by the caller.
func NewScanner(cfg ScannerConfig) *Scanner {
	if cfg.Workers <= 0 {
		cfg.Workers = min(runtime.NumCPU(), 8)
	}
	return &Scanner{
		cfg:   cfg,
		tasks: make(chan FileTask, cfg.Workers*4),
		errs:  make(chan error, cfg.Workers*4),
	}
}

// Scan starts the scanner and returns channels for tasks and errors.
// The caller must consume from both channels until they close.
func (s *Scanner) Scan(ctx context.Context) (<-chan FileTask, <-chan error) {
	go func() {
		defer close(s.tasks)
		defer close(s.errs)
		s.scanTree(ctx)
	}()

	return s.tasks, s.errs
}

func (s *Scanner) scanTree(ctx context.Context) {
	s.emit(event.Event{Type: event.ScanStarted})

	// Remote sources: use endpoint Walk (single streaming RPC) instead of
	// parallel os.ReadDir which would fail for non-local paths.
	if _, isLocal := s.cfg.SrcEndpoint.(transport.PathResolver); !isLocal {
		s.scanTreeRemote(ctx)
		return
	}

	workQueue := make(chan string, s.cfg.Workers*2)
	var outstanding sync.WaitGroup // tracks directories queued but not yet processed

	// Start workers.
	var workerWg sync.WaitGroup
	for range s.cfg.Workers {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for dirPath := range workQueue {
				s.scanDir(ctx, dirPath, workQueue, &outstanding)
				outstanding.Done()
			}
		}()
	}

	// Seed with root.
	outstanding.Add(1)
	workQueue <- s.cfg.SrcRoot

	// Wait for all directory work to finish, then close the work queue
	// so workers exit their range loop.
	outstanding.Wait()
	close(workQueue)
	workerWg.Wait()
}

// scanTreeRemote walks the source tree via SrcEndpoint.Walk() for remote
// sources (beam://, SFTP). This replaces the parallel os.ReadDir scanner
// with a single streaming RPC.
//
//nolint:revive // cognitive-complexity: type-switch dispatch with filter, skip detection, and task emission
func (s *Scanner) scanTreeRemote(ctx context.Context) {
	slog.Debug("starting remote walk", "endpoint_root", s.cfg.SrcEndpoint.Root())
	err := s.cfg.SrcEndpoint.Walk(func(entry transport.FileEntry) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		srcPath := filepath.Join(s.cfg.SrcRoot, entry.RelPath)
		dstPath := filepath.Join(s.cfg.DstRoot, entry.RelPath)

		// Apply filter if configured.
		if s.cfg.Filter != nil {
			if !s.cfg.Filter.Match(entry.RelPath, entry.IsDir, entry.Size) {
				s.emit(event.Event{Type: event.FileSkipped, Path: entry.RelPath, Size: entry.Size})
				return nil
			}
		}

		switch {
		case entry.IsDir:
			s.sendTask(FileTask{
				SrcPath: srcPath,
				DstPath: dstPath,
				Type:    Dir,
				Mode:    uint32(entry.Mode),
				UID:     entry.UID,
				GID:     entry.GID,
				ModTime: entry.ModTime,
				AccTime: entry.AccTime,
			})

		case entry.IsSymlink:
			s.sendTask(FileTask{
				SrcPath:    srcPath,
				DstPath:    dstPath,
				Type:       Symlink,
				Mode:       uint32(entry.Mode),
				UID:        entry.UID,
				GID:        entry.GID,
				ModTime:    entry.ModTime,
				AccTime:    entry.AccTime,
				LinkTarget: entry.LinkTarget,
			})

		case entry.Mode.IsRegular():
			if err := s.processRegularRemote(srcPath, dstPath, entry); err != nil {
				s.sendErr(err)
			}
		}

		return nil
	})
	if err != nil && ctx.Err() == nil {
		s.sendErr(fmt.Errorf("remote walk: %w", err))
	}
	slog.Debug("remote walk complete", "files", s.totalFiles.Load(), "bytes", s.totalBytes.Load())
}

// processRegularRemote handles skip detection and task emission for a regular
// file discovered via remote Walk. Unlike processRegular, it does not perform
// sparse detection or hardlink tracking (no local fd / unreliable across transports).
func (s *Scanner) processRegularRemote(
	srcPath, dstPath string,
	entry transport.FileEntry,
) error {
	// Skip detection using DstIndex or DstEndpoint.Stat().
	relDst, err := filepath.Rel(s.cfg.DstRoot, dstPath)
	if err != nil {
		return fmt.Errorf("rel path %s: %w", dstPath, err)
	}

	var dstEntry transport.FileEntry
	var dstFound bool
	if s.cfg.DstIndex != nil {
		dstEntry, dstFound = s.cfg.DstIndex[relDst]
	} else {
		dstEntry, err = s.cfg.DstEndpoint.Stat(relDst)
		dstFound = err == nil
	}

	if dstFound && dstEntry.Size == entry.Size && dstEntry.ModTime.Equal(entry.ModTime) {
		s.emit(event.Event{Type: event.FileSkipped, Path: entry.RelPath, Size: entry.Size})
		if s.cfg.Stats != nil {
			s.cfg.Stats.AddFilesSkipped(1)
			s.cfg.Stats.AddBytesCopied(entry.Size)
		}
		return nil
	}

	var chunks []Chunk
	if s.cfg.ChunkThreshold > 0 && entry.Size > s.cfg.ChunkThreshold {
		chunks = splitIntoChunks(entry.Size, s.cfg.ChunkThreshold)
	}

	s.totalFiles.Add(1)
	s.totalBytes.Add(entry.Size)
	s.sendTask(FileTask{
		SrcPath: srcPath,
		DstPath: dstPath,
		Type:    Regular,
		Size:    entry.Size,
		Mode:    uint32(entry.Mode),
		UID:     entry.UID,
		GID:     entry.GID,
		ModTime: entry.ModTime,
		AccTime: entry.AccTime,
		Chunks:  chunks,
	})
	return nil
}

//nolint:revive // cognitive-complexity: directory scan with stat, filter, and task emission
func (s *Scanner) scanDir(
	ctx context.Context,
	srcPath string,
	workQueue chan<- string,
	outstanding *sync.WaitGroup,
) {
	relPath, err := filepath.Rel(s.cfg.SrcRoot, srcPath)
	if err != nil {
		s.sendErr(fmt.Errorf("rel path for %s: %w", srcPath, err))
		return
	}

	dstPath := filepath.Join(s.cfg.DstRoot, relPath)

	info, err := os.Lstat(srcPath)
	if err != nil {
		s.sendErr(fmt.Errorf("lstat %s: %w", srcPath, err))
		return
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		s.sendErr(fmt.Errorf("unsupported stat type for %s", srcPath))
		return
	}

	// Emit directory task (except root, which the caller creates).
	if srcPath != s.cfg.SrcRoot {
		s.sendTask(FileTask{
			SrcPath: srcPath,
			DstPath: dstPath,
			Type:    Dir,
			Mode:    uint32(info.Mode()),
			UID:     stat.Uid,
			GID:     stat.Gid,
			ModTime: info.ModTime(),
			AccTime: atimeFromStat(stat),
		})
	}

	entries, err := os.ReadDir(srcPath)
	if err != nil {
		s.sendErr(fmt.Errorf("readdir %s: %w", srcPath, err))
		return
	}

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return
		default:
		}

		entryPath := filepath.Join(srcPath, entry.Name())
		entryDst := filepath.Join(dstPath, entry.Name())

		if err := s.processEntry(ctx, entryPath, entryDst, workQueue, outstanding); err != nil {
			s.sendErr(err)
		}
	}
}

//nolint:revive // cognitive-complexity: type-switch dispatch for dirs, symlinks, regulars
func (s *Scanner) processEntry(
	ctx context.Context,
	srcPath, dstPath string,
	workQueue chan<- string,
	outstanding *sync.WaitGroup,
) error {
	info, err := os.Lstat(srcPath)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", srcPath, err)
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unsupported stat type for %s", srcPath)
	}
	mode := info.Mode()

	// Apply filter if configured.
	if s.cfg.Filter != nil {
		relPath, relErr := filepath.Rel(s.cfg.SrcRoot, srcPath)
		if relErr != nil {
			return fmt.Errorf("rel path %s: %w", srcPath, relErr)
		}
		if !s.cfg.Filter.Match(relPath, mode.IsDir(), info.Size()) {
			s.emit(event.Event{Type: event.FileSkipped, Path: relPath, Size: info.Size()})
			return nil
		}
	}

	switch {
	case mode.IsDir():
		outstanding.Add(1)
		select {
		case workQueue <- srcPath:
		case <-ctx.Done():
			outstanding.Done()
			return ctx.Err()
		}
		return nil

	case mode&os.ModeSymlink != 0:
		target, err := os.Readlink(srcPath)
		if err != nil {
			return fmt.Errorf("readlink %s: %w", srcPath, err)
		}
		s.sendTask(FileTask{
			SrcPath:    srcPath,
			DstPath:    dstPath,
			Type:       Symlink,
			Mode:       uint32(mode),
			UID:        stat.Uid,
			GID:        stat.Gid,
			ModTime:    info.ModTime(),
			AccTime:    atimeFromStat(stat),
			LinkTarget: target,
		})
		return nil

	case mode.IsRegular():
		return s.processRegular(srcPath, dstPath, info, stat)

	default:
		return nil
	}
}

//nolint:gocyclo,revive // skip detection, hardlink tracking, sparse detection, chunking
func (s *Scanner) processRegular(
	srcPath, dstPath string,
	info os.FileInfo,
	stat *syscall.Stat_t,
) error {
	mode := info.Mode()

	// Skip files where the destination already exists with matching
	// size and mtime. When DstIndex is set, check it (O(1) local map lookup)
	// instead of making per-file Stat RPCs.
	relDst, err := filepath.Rel(s.cfg.DstRoot, dstPath)
	if err != nil {
		return fmt.Errorf("rel path %s: %w", dstPath, err)
	}
	var dstEntry transport.FileEntry
	var dstFound bool
	if s.cfg.DstIndex != nil {
		dstEntry, dstFound = s.cfg.DstIndex[relDst]
	} else {
		dstEntry, err = s.cfg.DstEndpoint.Stat(relDst)
		dstFound = err == nil
	}
	if dstFound {
		if dstEntry.Size == info.Size() && dstEntry.ModTime.Equal(info.ModTime()) {
			relPath, relErr := filepath.Rel(s.cfg.SrcRoot, srcPath)
			if relErr != nil {
				return fmt.Errorf("rel path %s: %w", srcPath, relErr)
			}
			s.emit(event.Event{Type: event.FileSkipped, Path: relPath, Size: info.Size()})
			if s.cfg.Stats != nil {
				s.cfg.Stats.AddFilesSkipped(1)
				s.cfg.Stats.AddBytesCopied(info.Size())
			}
			return nil
		}
	}

	// Hardlink detection: only when capabilities support it.
	sparseCapable := s.cfg.SrcEndpoint.Caps().SparseDetect
	hardlinkCapable := s.cfg.SrcEndpoint.Caps().Hardlinks

	devino := DevIno{Dev: devFromStat(stat), Ino: stat.Ino}
	if hardlinkCapable && stat.Nlink > 1 {
		if firstPath, seen := s.inodeSeen.LoadOrStore(devino, srcPath); seen {
			s.sendTask(FileTask{
				SrcPath:    srcPath,
				DstPath:    dstPath,
				Type:       Hardlink,
				LinkTarget: firstPath.(string),
				DevIno:     devino,
			})
			return nil
		}
	}

	var segments []Segment
	if s.cfg.SparseDetect && sparseCapable && info.Size() > 0 {
		fd, err := os.Open(srcPath)
		if err != nil {
			return fmt.Errorf("open %s for sparse detection: %w", srcPath, err)
		}
		segments, err = DetectSparseSegments(fd, info.Size())
		fd.Close()
		if err != nil {
			return fmt.Errorf("detect sparse %s: %w", srcPath, err)
		}
	}

	var chunks []Chunk
	if s.cfg.ChunkThreshold > 0 && info.Size() > s.cfg.ChunkThreshold {
		chunks = splitIntoChunks(info.Size(), s.cfg.ChunkThreshold)
	}

	s.totalFiles.Add(1)
	s.totalBytes.Add(info.Size())
	s.sendTask(FileTask{
		SrcPath:  srcPath,
		DstPath:  dstPath,
		Type:     Regular,
		Size:     info.Size(),
		Mode:     uint32(mode),
		UID:      stat.Uid,
		GID:      stat.Gid,
		ModTime:  info.ModTime(),
		AccTime:  atimeFromStat(stat),
		DevIno:   devino,
		Segments: segments,
		Chunks:   chunks,
	})
	return nil
}

func (s *Scanner) sendTask(task FileTask) {
	s.tasks <- task
}

func (s *Scanner) sendErr(err error) {
	select {
	case s.errs <- err:
	default:
	}
}

func (s *Scanner) emit(e event.Event) {
	if s.cfg.Events == nil {
		return
	}
	e.Timestamp = time.Now()
	select {
	case s.cfg.Events <- e:
	default:
	}
}

// Prescan walks the source tree counting files and bytes without building tasks.
// It's much faster than a full scan (just readdir+lstat, no file opens or sparse
// detection) and provides accurate totals for the progress display.
//
//nolint:revive // cognitive-complexity: parallel directory walk with filter and aggregation
func Prescan(ctx context.Context, root string, f *filter.Chain) (files int64, bytes int64) {
	type result struct {
		files int64
		bytes int64
	}

	workers := min(runtime.NumCPU(), 8)
	workQueue := make(chan string, workers*4)
	results := make(chan result, workers*4)
	var outstanding sync.WaitGroup

	// Prescan workers.
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for dir := range workQueue {
				entries, err := os.ReadDir(dir)
				if err != nil {
					outstanding.Done()
					continue
				}
				var localFiles, localBytes int64
				for _, e := range entries {
					select {
					case <-ctx.Done():
						outstanding.Done()
						return
					default:
					}
					path := filepath.Join(dir, e.Name())
					info, err := os.Lstat(path)
					if err != nil {
						continue
					}
					mode := info.Mode()

					// Apply filter.
					if f != nil {
						relPath, relErr := filepath.Rel(root, path)
						if relErr != nil || !f.Match(relPath, mode.IsDir(), info.Size()) {
							continue
						}
					}

					if mode.IsDir() {
						outstanding.Add(1)
						workQueue <- path
					} else if mode.IsRegular() {
						localFiles++
						localBytes += info.Size()
					}
				}
				if localFiles > 0 {
					results <- result{localFiles, localBytes}
				}
				outstanding.Done()
			}
		})
	}

	// Seed root.
	outstanding.Add(1)
	workQueue <- root

	// Close work queue when all dirs processed, then close results.
	go func() {
		outstanding.Wait()
		close(workQueue)
		wg.Wait()
		close(results)
	}()

	for r := range results {
		files += r.files
		bytes += r.bytes
	}
	return files, bytes
}

func splitIntoChunks(fileSize, chunkSize int64) []Chunk {
	var chunks []Chunk
	offset := int64(0)
	for offset < fileSize {
		length := chunkSize
		if offset+length > fileSize {
			length = fileSize - offset
		}
		chunks = append(chunks, Chunk{Offset: offset, Length: length})
		offset += length
	}
	return chunks
}
