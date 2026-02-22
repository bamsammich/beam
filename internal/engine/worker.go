package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/platform"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
	"golang.org/x/sys/unix"
)

// WorkerConfig controls worker behavior.
type WorkerConfig struct {
	NumWorkers    int
	PreserveMode  bool
	PreserveTimes bool
	PreserveOwner bool
	PreserveXattr bool
	NoTimes       bool // disable all time preservation including default mtime
	DryRun        bool
	UseIOURing    bool
	Stats         stats.Writer
	Events        chan<- event.Event
	DstRoot       string        // destination root for computing relative paths
	WorkerLimit   *atomic.Int32 // runtime throttle; nil = all workers active
	SrcEndpoint   transport.ReadEndpoint
	DstEndpoint   transport.WriteEndpoint
	Delta         bool // use delta transfer for remote copies
}

// WorkerPool manages a pool of copy workers.
type WorkerPool struct {
	cfg       WorkerConfig
	iouring   *platform.IOURingCopier
	localFast bool // true when both endpoints are local — enables kernel copy offload
}

// NewWorkerPool creates a new worker pool.
// Both SrcEndpoint and DstEndpoint must be set; DstRoot must be non-empty.
func NewWorkerPool(cfg WorkerConfig) (*WorkerPool, error) {
	wp := &WorkerPool{cfg: cfg}

	// Detect local fast-path for the data copy: kernel copy offload
	// (copy_file_range, sendfile, io_uring) requires raw fds.
	wp.localFast = isLocalEndpoints(cfg.SrcEndpoint, cfg.DstEndpoint)

	if cfg.UseIOURing && wp.localFast {
		copier, err := platform.NewIOURingCopier(64)
		if err != nil {
			return nil, fmt.Errorf("init io_uring: %w", err)
		}
		wp.iouring = copier // may be nil if kernel too old
	}

	return wp, nil
}

// isLocalEndpoints returns true when both endpoints are local.
func isLocalEndpoints(src transport.ReadEndpoint, dst transport.WriteEndpoint) bool {
	_, srcLocal := src.(*transport.LocalReadEndpoint)
	_, dstLocal := dst.(*transport.LocalWriteEndpoint)
	return srcLocal && dstLocal
}

// Run starts workers that consume tasks. It blocks until all tasks are
// processed or the context is cancelled. Errors are sent to errs.
func (wp *WorkerPool) Run(ctx context.Context, tasks <-chan FileTask, errs chan<- error) {
	var wg sync.WaitGroup
	for i := range wp.cfg.NumWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				// Throttle: wait if this worker's ID is at or above the limit.
				if !wp.waitForThrottle(ctx, i) {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := wp.processTask(ctx, task, i); err != nil {
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

// waitForThrottle blocks until this worker is allowed to proceed.
// Returns false if the context was cancelled.
func (wp *WorkerPool) waitForThrottle(ctx context.Context, workerID int) bool {
	if wp.cfg.WorkerLimit == nil {
		return true
	}
	for int32(workerID) >= wp.cfg.WorkerLimit.Load() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	return true
}

func (wp *WorkerPool) emit(e event.Event) {
	if wp.cfg.Events == nil {
		return
	}
	e.Timestamp = time.Now()
	select {
	case wp.cfg.Events <- e:
	default:
	}
}

// Close cleans up resources.
func (wp *WorkerPool) Close() {
	CleanupTmpFiles()
	if wp.iouring != nil {
		wp.iouring.Close()
	}
}

func (wp *WorkerPool) processTask(ctx context.Context, task FileTask, workerID int) error {
	if wp.cfg.DryRun {
		wp.cfg.Stats.AddFilesScanned(1)
		wp.emit(event.Event{Type: event.FileSkipped, Path: task.DstPath, Size: task.Size, WorkerID: workerID})
		return nil
	}

	switch task.Type {
	case Dir:
		return wp.createDirectory(task, workerID)
	case Symlink:
		return wp.createSymlink(task)
	case Hardlink:
		return wp.createHardlink(task, workerID)
	case Regular:
		return wp.copyRegularFile(ctx, task, workerID)
	default:
		return fmt.Errorf("unknown task type %d for %s", task.Type, task.SrcPath)
	}
}

// relDst computes the destination-relative path for a task.
func (wp *WorkerPool) relDst(absPath string) string {
	rel, _ := filepath.Rel(wp.cfg.DstRoot, absPath)
	return rel
}

func (wp *WorkerPool) createDirectory(task FileTask, workerID int) error {
	relDst := wp.relDst(task.DstPath)
	if err := wp.cfg.DstEndpoint.MkdirAll(relDst, os.FileMode(task.Mode).Perm()); err != nil {
		return fmt.Errorf("mkdir %s: %w", task.DstPath, err)
	}

	if wp.cfg.PreserveMode || wp.cfg.PreserveTimes || wp.cfg.PreserveOwner {
		entry := taskToEntry(task)
		opts := transport.MetadataOpts{
			Mode:  wp.cfg.PreserveMode,
			Times: wp.cfg.PreserveTimes,
			Owner: wp.cfg.PreserveOwner,
		}
		if err := wp.cfg.DstEndpoint.SetMetadata(relDst, entry, opts); err != nil {
			return err
		}
	}

	wp.cfg.Stats.AddDirsCreated(1)
	wp.emit(event.Event{Type: event.DirCreated, Path: task.DstPath, WorkerID: workerID})
	return nil
}

func (wp *WorkerPool) createSymlink(task FileTask) error {
	relDst := wp.relDst(task.DstPath)
	relDir := filepath.Dir(relDst)
	if relDir != "." {
		if err := wp.cfg.DstEndpoint.MkdirAll(relDir, 0755); err != nil {
			return fmt.Errorf("create parent dir for symlink %s: %w", task.DstPath, err)
		}
	}
	if err := wp.cfg.DstEndpoint.Symlink(task.LinkTarget, relDst); err != nil {
		return fmt.Errorf("symlink %s -> %s: %w", task.DstPath, task.LinkTarget, err)
	}

	wp.cfg.Stats.AddFilesCopied(1)
	return nil
}

func (wp *WorkerPool) createHardlink(task FileTask, workerID int) error {
	// Translate the source link target to the destination path.
	// task.LinkTarget is the source path of the first copy; we need
	// the corresponding destination path.
	relTarget, err := filepath.Rel(filepath.Dir(task.SrcPath), task.LinkTarget)
	if err != nil {
		return fmt.Errorf("rel hardlink target: %w", err)
	}
	dstTarget := filepath.Join(filepath.Dir(task.DstPath), relTarget)

	relDst := wp.relDst(task.DstPath)
	relDstTarget := wp.relDst(dstTarget)
	relDir := filepath.Dir(relDst)
	if relDir != "." {
		if err := wp.cfg.DstEndpoint.MkdirAll(relDir, 0755); err != nil {
			return fmt.Errorf("create parent dir for hardlink %s: %w", task.DstPath, err)
		}
	}
	if err := wp.cfg.DstEndpoint.Link(relDstTarget, relDst); err != nil {
		return fmt.Errorf("hardlink %s -> %s: %w", task.DstPath, dstTarget, err)
	}

	wp.cfg.Stats.AddHardlinksCreated(1)
	wp.emit(event.Event{Type: event.HardlinkCreated, Path: task.DstPath, WorkerID: workerID})
	return nil
}

func (wp *WorkerPool) copyRegularFile(ctx context.Context, task FileTask, workerID int) error {
	wp.cfg.Stats.AddFilesScanned(1)
	wp.emit(event.Event{Type: event.FileStarted, Path: task.DstPath, Size: task.Size, WorkerID: workerID})

	relDst := wp.relDst(task.DstPath)

	// Ensure parent directory exists (may race with dir task workers).
	relDir := filepath.Dir(relDst)
	if relDir != "." {
		if err := wp.cfg.DstEndpoint.MkdirAll(relDir, 0755); err != nil {
			wp.cfg.Stats.AddFilesFailed(1)
			wp.emit(event.Event{Type: event.FileFailed, Path: task.DstPath, Size: task.Size, Error: err, WorkerID: workerID})
			return fmt.Errorf("create parent dir %s: %w", relDir, err)
		}
	}

	// Create temp file on destination via endpoint.
	tmpFile, err := wp.cfg.DstEndpoint.CreateTemp(relDst, os.FileMode(task.Mode).Perm())
	if err != nil {
		wp.cfg.Stats.AddFilesFailed(1)
		wp.emit(event.Event{Type: event.FileFailed, Path: task.DstPath, Size: task.Size, Error: err, WorkerID: workerID})
		return fmt.Errorf("create temp for %s: %w", relDst, err)
	}

	tmpRelPath := tmpFile.Name()

	// For local endpoints, register the tmp file for crash cleanup.
	localTmpPath := ""
	if localDst, ok := wp.cfg.DstEndpoint.(*transport.LocalWriteEndpoint); ok {
		localTmpPath = localDst.AbsPath(tmpRelPath)
		RegisterTmp(localTmpPath)
	}
	defer func() {
		if localTmpPath != "" {
			DeregisterTmp(localTmpPath)
		}
		_ = wp.cfg.DstEndpoint.Remove(tmpRelPath)
	}()

	// Copy data.
	var totalBytes int64
	if task.Size > 0 {
		totalBytes, err = wp.copyFileData(task, tmpFile)
		if err != nil {
			tmpFile.Close()
			wp.cfg.Stats.AddFilesFailed(1)
			wp.emit(event.Event{Type: event.FileFailed, Path: task.DstPath, Size: task.Size, Error: err, WorkerID: workerID})
			return fmt.Errorf("copy data %s: %w", task.SrcPath, err)
		}
	}

	// Set metadata before rename.
	if err := wp.setMetadata(task, tmpFile, tmpRelPath); err != nil {
		tmpFile.Close()
		wp.cfg.Stats.AddFilesFailed(1)
		wp.emit(event.Event{Type: event.FileFailed, Path: task.DstPath, Size: task.Size, Error: err, WorkerID: workerID})
		return fmt.Errorf("set metadata %s: %w", task.DstPath, err)
	}

	if err := tmpFile.Close(); err != nil {
		wp.cfg.Stats.AddFilesFailed(1)
		wp.emit(event.Event{Type: event.FileFailed, Path: task.DstPath, Size: task.Size, Error: err, WorkerID: workerID})
		return fmt.Errorf("close temp %s: %w", tmpRelPath, err)
	}

	// Atomic rename.
	if err := wp.cfg.DstEndpoint.Rename(tmpRelPath, relDst); err != nil {
		wp.cfg.Stats.AddFilesFailed(1)
		wp.emit(event.Event{Type: event.FileFailed, Path: task.DstPath, Size: task.Size, Error: err, WorkerID: workerID})
		return fmt.Errorf("rename %s -> %s: %w", tmpRelPath, relDst, err)
	}

	wp.cfg.Stats.AddFilesCopied(1)
	wp.cfg.Stats.AddBytesCopied(totalBytes)
	wp.emit(event.Event{Type: event.FileCompleted, Path: task.DstPath, Size: totalBytes, WorkerID: workerID})

	return nil
}

// copyFileData copies file content from source to destination. When both
// endpoints are local, it extracts the raw *os.File and uses kernel copy
// offload (copy_file_range, sendfile, io_uring). Otherwise it streams via
// io.Copy, with optional delta transfer for remote destinations.
func (wp *WorkerPool) copyFileData(task FileTask, tmpFile transport.WriteFile) (int64, error) {
	// Local fast-path: extract raw fd for kernel copy offload.
	if dstFd := transport.LocalFile(tmpFile); dstFd != nil && wp.localFast {
		return wp.copyDataLocal(task, dstFd)
	}

	// Delta transfer: when enabled and a basis file exists on the destination.
	if wp.cfg.Delta && task.Size >= transport.DeltaMinFileSize() {
		relDst := wp.relDst(task.DstPath)
		if dstEntry, err := wp.cfg.DstEndpoint.Stat(relDst); err == nil && dstEntry.Size > 0 {
			n, err := wp.copyDataDelta(task, tmpFile, relDst)
			if err == nil {
				return n, nil
			}
			// Delta failed — fall through to full stream copy.
		}
	}

	// Stream path: read from source endpoint, write to dest file.
	relSrc, _ := filepath.Rel(wp.cfg.SrcEndpoint.Root(), task.SrcPath)
	srcReader, err := wp.cfg.SrcEndpoint.OpenRead(relSrc)
	if err != nil {
		return 0, fmt.Errorf("open source %s: %w", relSrc, err)
	}
	defer srcReader.Close()

	n, err := io.Copy(tmpFile, srcReader)
	return n, err
}

// copyDataDelta uses rsync-style delta transfer: compute signatures of the
// existing destination file, match blocks against the source, and write only
// changed regions to the temp file.
func (wp *WorkerPool) copyDataDelta(task FileTask, tmpFile transport.WriteFile, relDst string) (int64, error) {
	// Read the existing destination file for block signatures.
	dstReader, err := wp.cfg.DstEndpoint.OpenRead(relDst)
	if err != nil {
		return 0, err
	}

	dstEntry, _ := wp.cfg.DstEndpoint.Stat(relDst)
	sig, err := transport.ComputeSignature(dstReader, dstEntry.Size)
	dstReader.Close()
	if err != nil {
		return 0, err
	}

	// Read source and match against basis signatures.
	relSrc, _ := filepath.Rel(wp.cfg.SrcEndpoint.Root(), task.SrcPath)
	srcReader, err := wp.cfg.SrcEndpoint.OpenRead(relSrc)
	if err != nil {
		return 0, err
	}

	ops, err := transport.MatchBlocks(srcReader, sig)
	srcReader.Close()
	if err != nil {
		return 0, err
	}

	// Re-open basis file for applying delta.
	basisReader, err := wp.cfg.DstEndpoint.OpenRead(relDst)
	if err != nil {
		return 0, err
	}
	defer basisReader.Close()

	// basisReader must support seeking for ApplyDelta.
	basisSeeker, ok := basisReader.(io.ReadSeeker)
	if !ok {
		return 0, fmt.Errorf("destination endpoint does not support seeking for delta")
	}

	countWriter := &countingWriter{w: tmpFile}
	if err := transport.ApplyDelta(basisSeeker, ops, countWriter); err != nil {
		return 0, err
	}

	return countWriter.n, nil
}

// countingWriter wraps an io.Writer and counts bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// copyDataLocal copies file data using platform fast-paths (copy_file_range,
// sendfile, io_uring). Handles sparse segments and chunked transfers.
func (wp *WorkerPool) copyDataLocal(task FileTask, dstFd *os.File) (int64, error) {
	// Sparse-aware copy: only copy data segments.
	if len(task.Segments) > 0 {
		return wp.copySegments(task, dstFd)
	}

	// Chunked copy for large files.
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

// setMetadata sets file metadata. For local endpoints, uses fd-based syscalls
// for xattr support. For remote endpoints, uses the endpoint's SetMetadata.
func (wp *WorkerPool) setMetadata(task FileTask, tmpFile transport.WriteFile, tmpRelPath string) error {
	// For local files, use fd-based operations for xattr and precise mtime control.
	if fd := transport.LocalFile(tmpFile); fd != nil && wp.localFast {
		return wp.setFileMetadataLocal(task, fd)
	}

	entry := taskToEntry(task)
	opts := transport.MetadataOpts{
		Mode:  wp.cfg.PreserveMode,
		Times: !wp.cfg.NoTimes,
		Owner: wp.cfg.PreserveOwner,
	}
	return wp.cfg.DstEndpoint.SetMetadata(tmpRelPath, entry, opts)
}

// setFileMetadataLocal sets metadata via raw fd syscalls (local fast path).
func (wp *WorkerPool) setFileMetadataLocal(task FileTask, fd *os.File) error {
	rawFd := int(fd.Fd())

	if wp.cfg.PreserveMode {
		if err := unix.Fchmod(rawFd, task.Mode&0o7777); err != nil {
			return fmt.Errorf("fchmod: %w", err)
		}
	}

	// Always preserve mtime so skip detection (size + mtime) works on re-runs.
	// In archive mode, also preserve atime. --no-times disables both.
	if !wp.cfg.NoTimes {
		atime := unix.Timespec{Nsec: unix.UTIME_OMIT}
		if wp.cfg.PreserveTimes {
			atime = unix.NsecToTimespec(task.AccTime.UnixNano())
		}
		times := []unix.Timespec{
			atime,
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

// taskToEntry converts a FileTask to a transport.FileEntry for metadata operations.
func taskToEntry(task FileTask) transport.FileEntry {
	return transport.FileEntry{
		Mode:    os.FileMode(task.Mode),
		ModTime: task.ModTime,
		AccTime: task.AccTime,
		Uid:     task.Uid,
		Gid:     task.Gid,
	}
}
