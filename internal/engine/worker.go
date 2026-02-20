package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/bamsammich/beam/internal/platform"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// WorkerConfig controls worker behavior.
type WorkerConfig struct {
	NumWorkers    int
	PreserveMode  bool
	PreserveTimes bool
	PreserveOwner bool
	PreserveXattr bool
	DryRun        bool
	UseIOURing    bool
	Stats         *stats.Collector
}

// WorkerPool manages a pool of copy workers.
type WorkerPool struct {
	cfg     WorkerConfig
	iouring *platform.IOURingCopier
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(cfg WorkerConfig) (*WorkerPool, error) {
	wp := &WorkerPool{cfg: cfg}

	if cfg.UseIOURing {
		copier, err := platform.NewIOURingCopier(64)
		if err != nil {
			return nil, fmt.Errorf("init io_uring: %w", err)
		}
		wp.iouring = copier // may be nil if kernel too old
	}

	return wp, nil
}

// Run starts workers that consume tasks. It blocks until all tasks are
// processed or the context is cancelled. Errors are sent to errs.
func (wp *WorkerPool) Run(ctx context.Context, tasks <-chan FileTask, errs chan<- error) {
	var wg sync.WaitGroup
	for range wp.cfg.NumWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := wp.processTask(ctx, task); err != nil {
					select {
					case errs <- err:
					default:
					}
				}
			}
		}()
	}
	wg.Wait()
}

// Close cleans up resources.
func (wp *WorkerPool) Close() {
	CleanupTmpFiles()
	if wp.iouring != nil {
		wp.iouring.Close()
	}
}

func (wp *WorkerPool) processTask(ctx context.Context, task FileTask) error {
	if wp.cfg.DryRun {
		wp.cfg.Stats.AddFilesScanned(1)
		return nil
	}

	switch task.Type {
	case Dir:
		return wp.createDirectory(task)
	case Symlink:
		return wp.createSymlink(task)
	case Hardlink:
		return wp.createHardlink(task)
	case Regular:
		return wp.copyRegularFile(ctx, task)
	default:
		return fmt.Errorf("unknown task type %d for %s", task.Type, task.SrcPath)
	}
}

func (wp *WorkerPool) createDirectory(task FileTask) error {
	err := os.MkdirAll(task.DstPath, os.FileMode(task.Mode).Perm())
	if err != nil {
		return fmt.Errorf("mkdir %s: %w", task.DstPath, err)
	}

	if wp.cfg.PreserveMode || wp.cfg.PreserveTimes || wp.cfg.PreserveOwner {
		if err := wp.setDirMetadata(task); err != nil {
			return err
		}
	}

	wp.cfg.Stats.AddDirsCreated(1)
	return nil
}

func (wp *WorkerPool) createSymlink(task FileTask) error {
	if err := os.MkdirAll(filepath.Dir(task.DstPath), 0755); err != nil {
		return fmt.Errorf("create parent dir for symlink %s: %w", task.DstPath, err)
	}
	_ = os.Remove(task.DstPath)

	if err := os.Symlink(task.LinkTarget, task.DstPath); err != nil {
		return fmt.Errorf("symlink %s -> %s: %w", task.DstPath, task.LinkTarget, err)
	}

	wp.cfg.Stats.AddFilesCopied(1)
	return nil
}

func (wp *WorkerPool) createHardlink(task FileTask) error {
	// Translate the source link target to the destination path.
	// task.LinkTarget is the source path of the first copy; we need
	// the corresponding destination path.
	relTarget, err := filepath.Rel(filepath.Dir(task.SrcPath), task.LinkTarget)
	if err != nil {
		return fmt.Errorf("rel hardlink target: %w", err)
	}
	dstTarget := filepath.Join(filepath.Dir(task.DstPath), relTarget)

	if err := os.MkdirAll(filepath.Dir(task.DstPath), 0755); err != nil {
		return fmt.Errorf("create parent dir for hardlink %s: %w", task.DstPath, err)
	}
	_ = os.Remove(task.DstPath)

	if err := os.Link(dstTarget, task.DstPath); err != nil {
		return fmt.Errorf("hardlink %s -> %s: %w", task.DstPath, dstTarget, err)
	}

	wp.cfg.Stats.AddHardlinksCreated(1)
	return nil
}

func (wp *WorkerPool) copyRegularFile(ctx context.Context, task FileTask) error {
	wp.cfg.Stats.AddFilesScanned(1)

	dir := filepath.Dir(task.DstPath)
	base := filepath.Base(task.DstPath)
	tmpName := fmt.Sprintf(".%s.%s.beam-tmp", base, uuid.New().String()[:8])
	tmpPath := filepath.Join(dir, tmpName)

	// Ensure parent directory exists (may race with dir task workers).
	if err := os.MkdirAll(dir, 0755); err != nil {
		wp.cfg.Stats.AddFilesFailed(1)
		return fmt.Errorf("create parent dir %s: %w", dir, err)
	}

	RegisterTmp(tmpPath)
	defer func() {
		DeregisterTmp(tmpPath)
		_ = os.Remove(tmpPath) // no-op if rename succeeded
	}()

	// Create tmp file.
	tmpFd, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(task.Mode).Perm())
	if err != nil {
		wp.cfg.Stats.AddFilesFailed(1)
		return fmt.Errorf("create tmp %s: %w", tmpPath, err)
	}

	// Copy data.
	var totalBytes int64
	if task.Size > 0 {
		totalBytes, err = wp.copyData(task, tmpFd)
		if err != nil {
			tmpFd.Close()
			wp.cfg.Stats.AddFilesFailed(1)
			return fmt.Errorf("copy data %s: %w", task.SrcPath, err)
		}
	}

	// Set metadata before rename.
	if err := wp.setFileMetadata(task, tmpFd); err != nil {
		tmpFd.Close()
		wp.cfg.Stats.AddFilesFailed(1)
		return fmt.Errorf("set metadata %s: %w", task.DstPath, err)
	}

	if err := tmpFd.Close(); err != nil {
		wp.cfg.Stats.AddFilesFailed(1)
		return fmt.Errorf("close tmp %s: %w", tmpPath, err)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, task.DstPath); err != nil {
		wp.cfg.Stats.AddFilesFailed(1)
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, task.DstPath, err)
	}

	wp.cfg.Stats.AddFilesCopied(1)
	wp.cfg.Stats.AddBytesCopied(totalBytes)
	return nil
}

func (wp *WorkerPool) copyData(task FileTask, dstFd *os.File) (int64, error) {
	// Sparse-aware copy: only copy data segments.
	if len(task.Segments) > 0 {
		return wp.copySegments(task, dstFd)
	}

	// Simple whole-file copy (possibly chunked).
	if len(task.Chunks) > 0 {
		var total int64
		for _, chunk := range task.Chunks {
			result, err := wp.doCopy(platform.CopyFileParams{
				SrcPath:   task.SrcPath,
				DstFd:     dstFd,
				SrcOffset: chunk.Offset,
				Length:    chunk.Length,
				SrcSize:   task.Size,
			})
			if err != nil {
				return total, err
			}
			total += result.BytesWritten
		}
		return total, nil
	}

	result, err := wp.doCopy(platform.CopyFileParams{
		SrcPath: task.SrcPath,
		DstFd:   dstFd,
		SrcSize: task.Size,
	})
	if err != nil {
		return 0, err
	}
	return result.BytesWritten, nil
}

func (wp *WorkerPool) copySegments(task FileTask, dstFd *os.File) (int64, error) {
	// Pre-allocate to create the sparse layout via ftruncate.
	if err := dstFd.Truncate(task.Size); err != nil {
		return 0, fmt.Errorf("truncate for sparse: %w", err)
	}

	var total int64
	for _, seg := range task.Segments {
		if !seg.IsData {
			continue // skip holes
		}
		result, err := wp.doCopy(platform.CopyFileParams{
			SrcPath:   task.SrcPath,
			DstFd:     dstFd,
			SrcOffset: seg.Offset,
			Length:    seg.Length,
			SrcSize:   task.Size,
		})
		if err != nil {
			return total, err
		}
		total += result.BytesWritten
	}
	return total, nil
}

func (wp *WorkerPool) doCopy(params platform.CopyFileParams) (platform.CopyResult, error) {
	if wp.iouring != nil {
		return wp.iouring.CopyFile(params)
	}
	return platform.CopyFile(params)
}

func (wp *WorkerPool) setFileMetadata(task FileTask, fd *os.File) error {
	rawFd := int(fd.Fd())

	if wp.cfg.PreserveMode {
		if err := unix.Fchmod(rawFd, task.Mode&0o7777); err != nil {
			return fmt.Errorf("fchmod: %w", err)
		}
	}

	if wp.cfg.PreserveTimes {
		times := []unix.Timespec{
			unix.NsecToTimespec(task.AccTime.UnixNano()),
			unix.NsecToTimespec(task.ModTime.UnixNano()),
		}
		if err := unix.UtimesNanoAt(rawFd, "", times, unix.AT_EMPTY_PATH); err != nil {
			// Fallback: some systems don't support AT_EMPTY_PATH.
			path := fd.Name()
			if err2 := unix.UtimesNanoAt(unix.AT_FDCWD, path, times, 0); err2 != nil {
				return fmt.Errorf("utimensat: %w", err)
			}
		}
	}

	if wp.cfg.PreserveXattr {
		if err := wp.copyXattrs(task.SrcPath, fd); err != nil {
			return err
		}
	}

	// Ownership last — may fail without CAP_CHOWN.
	if wp.cfg.PreserveOwner {
		_ = unix.Fchown(rawFd, int(task.Uid), int(task.Gid))
	}

	return nil
}

func (wp *WorkerPool) setDirMetadata(task FileTask) error {
	if wp.cfg.PreserveMode {
		if err := os.Chmod(task.DstPath, os.FileMode(task.Mode).Perm()); err != nil {
			return fmt.Errorf("chmod dir %s: %w", task.DstPath, err)
		}
	}

	if wp.cfg.PreserveTimes {
		times := []unix.Timespec{
			unix.NsecToTimespec(task.AccTime.UnixNano()),
			unix.NsecToTimespec(task.ModTime.UnixNano()),
		}
		if err := unix.UtimesNanoAt(unix.AT_FDCWD, task.DstPath, times, 0); err != nil {
			return fmt.Errorf("utimensat dir %s: %w", task.DstPath, err)
		}
	}

	if wp.cfg.PreserveOwner {
		_ = syscall.Lchown(task.DstPath, int(task.Uid), int(task.Gid))
	}

	return nil
}

func (wp *WorkerPool) copyXattrs(srcPath string, dstFd *os.File) error {
	// List xattrs on source.
	sz, err := unix.Listxattr(srcPath, nil)
	if err != nil || sz == 0 {
		return nil // no xattrs or not supported
	}

	buf := make([]byte, sz)
	sz, err = unix.Listxattr(srcPath, buf)
	if err != nil {
		return nil
	}

	dstRawFd := int(dstFd.Fd())

	// Parse null-separated attribute names.
	for _, name := range parseXattrNames(buf[:sz]) {
		val, err := getXattr(srcPath, name)
		if err != nil {
			continue
		}
		_ = unix.Fsetxattr(dstRawFd, name, val, 0)
	}

	return nil
}

func getXattr(path, name string) ([]byte, error) {
	sz, err := unix.Getxattr(path, name, nil)
	if err != nil || sz == 0 {
		return nil, err
	}
	buf := make([]byte, sz)
	_, err = unix.Getxattr(path, name, buf)
	return buf, err
}

func parseXattrNames(buf []byte) []string {
	var names []string
	start := 0
	for i, b := range buf {
		if b == 0 {
			if i > start {
				names = append(names, string(buf[start:i]))
			}
			start = i + 1
		}
	}
	return names
}
