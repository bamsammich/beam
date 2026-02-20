package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// ScannerConfig controls scanner behavior.
type ScannerConfig struct {
	SrcRoot        string
	DstRoot        string
	Workers        int
	ChunkThreshold int64
	FollowSymlinks bool
	IncludeXattrs  bool
	SparseDetect   bool
}

// Scanner traverses a directory tree in parallel and emits FileTask items.
type Scanner struct {
	cfg       ScannerConfig
	tasks     chan FileTask
	errs      chan error
	inodeSeen sync.Map // DevIno -> string (first path seen)
}

// NewScanner creates a scanner with the given config.
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

func (s *Scanner) scanDir(ctx context.Context, srcPath string, workQueue chan<- string, outstanding *sync.WaitGroup) {
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

	stat := info.Sys().(*syscall.Stat_t)

	// Emit directory task (except root, which the caller creates).
	if srcPath != s.cfg.SrcRoot {
		s.sendTask(FileTask{
			SrcPath: srcPath,
			DstPath: dstPath,
			Type:    Dir,
			Mode:    uint32(info.Mode()),
			Uid:     stat.Uid,
			Gid:     stat.Gid,
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

func (s *Scanner) processEntry(ctx context.Context, srcPath, dstPath string, workQueue chan<- string, outstanding *sync.WaitGroup) error {
	info, err := os.Lstat(srcPath)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", srcPath, err)
	}

	stat := info.Sys().(*syscall.Stat_t)
	mode := info.Mode()

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
			Uid:        stat.Uid,
			Gid:        stat.Gid,
			ModTime:    info.ModTime(),
			AccTime:    atimeFromStat(stat),
			LinkTarget: target,
		})
		return nil

	case mode.IsRegular():
		devino := DevIno{Dev: stat.Dev, Ino: stat.Ino}
		if stat.Nlink > 1 {
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
		if s.cfg.SparseDetect && info.Size() > 0 {
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

		s.sendTask(FileTask{
			SrcPath:  srcPath,
			DstPath:  dstPath,
			Type:     Regular,
			Size:     info.Size(),
			Mode:     uint32(mode),
			Uid:      stat.Uid,
			Gid:      stat.Gid,
			ModTime:  info.ModTime(),
			AccTime:  atimeFromStat(stat),
			DevIno:   devino,
			Segments: segments,
			Chunks:   chunks,
		})
		return nil

	default:
		return nil
	}
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

func atimeFromStat(stat *syscall.Stat_t) time.Time {
	return time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
