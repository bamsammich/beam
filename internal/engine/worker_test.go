package engine

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
)

func newTestWorkerPool(
	t *testing.T,
	dstRoot string,
	opts ...func(*WorkerConfig),
) (*WorkerPool, *stats.Collector) {
	t.Helper()
	s := &stats.Collector{}
	cfg := WorkerConfig{
		NumWorkers:    2,
		PreserveMode:  true,
		PreserveTimes: true,
		Stats:         s,
		DstRoot:       dstRoot,
		SrcEndpoint:   transport.NewLocalReader("/"),
		DstEndpoint:   transport.NewLocalWriter(dstRoot),
	}
	for _, o := range opts {
		o(&cfg)
	}
	wp, err := NewWorkerPool(cfg)
	require.NoError(t, err)
	t.Cleanup(wp.Close)
	return wp, s
}

func TestWorker_SingleFileCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))

	data := []byte("hello, beam workers!")
	srcFile := filepath.Join(src, "file.txt")
	require.NoError(t, os.WriteFile(srcFile, data, 0644))
	dstFile := filepath.Join(dst, "file.txt")

	wp, s := newTestWorkerPool(t, dst)

	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)

	info, err := os.Stat(srcFile)
	require.NoError(t, err)
	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected *syscall.Stat_t")

	tasks <- FileTask{
		SrcPath: srcFile,
		DstPath: dstFile,
		Type:    Regular,
		Size:    info.Size(),
		Mode:    uint32(info.Mode()),
		UID:     stat.Uid,
		GID:     stat.Gid,
		ModTime: info.ModTime(),
		AccTime: time.Now(),
	}
	close(tasks)

	wp.Run(context.Background(), tasks, errs)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(dstFile)
	require.NoError(t, err)
	assert.Equal(t, data, got)

	snap := s.Snapshot()
	assert.Equal(t, int64(1), snap.FilesCopied)
	assert.Equal(t, int64(len(data)), snap.BytesCopied)
}

func TestWorker_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))

	data := []byte("atomic write test")
	srcFile := filepath.Join(src, "file.txt")
	require.NoError(t, os.WriteFile(srcFile, data, 0644))

	// Pre-create destination with different content.
	dstFile := filepath.Join(dst, "file.txt")
	require.NoError(t, os.WriteFile(dstFile, []byte("old content"), 0644))

	wp, _ := newTestWorkerPool(t, dst)

	info, err := os.Stat(srcFile)
	require.NoError(t, err)
	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected *syscall.Stat_t")

	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)
	tasks <- FileTask{
		SrcPath: srcFile,
		DstPath: dstFile,
		Type:    Regular,
		Size:    info.Size(),
		Mode:    uint32(info.Mode()),
		UID:     stat.Uid,
		GID:     stat.Gid,
		ModTime: info.ModTime(),
		AccTime: time.Now(),
	}
	close(tasks)

	wp.Run(context.Background(), tasks, errs)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(dstFile)
	require.NoError(t, err)
	assert.Equal(t, data, got)

	// Verify no .beam-tmp files remain.
	entries, err := os.ReadDir(dst)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".beam-tmp")
	}
}

func TestWorker_SparseFileCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))

	// Create a sparse file: 1MB hole, then 4KB data.
	srcFile := filepath.Join(src, "sparse.bin")
	fd, err := os.OpenFile(srcFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	fileSize := int64(1024*1024 + 4096)
	require.NoError(t, fd.Truncate(fileSize))
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAB
	}
	_, err = unix.Pwrite(int(fd.Fd()), data, 1024*1024)
	require.NoError(t, err)
	fd.Close()

	// Detect sparse segments.
	rfd, err := os.Open(srcFile)
	require.NoError(t, err)
	segments, err := DetectSparseSegments(rfd, fileSize)
	require.NoError(t, err)
	rfd.Close()

	info, err := os.Stat(srcFile)
	require.NoError(t, err)
	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected *syscall.Stat_t")

	dstFile := filepath.Join(dst, "sparse.bin")

	wp, _ := newTestWorkerPool(t, dst)
	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)
	tasks <- FileTask{
		SrcPath:  srcFile,
		DstPath:  dstFile,
		Type:     Regular,
		Size:     fileSize,
		Mode:     uint32(info.Mode()),
		UID:      stat.Uid,
		GID:      stat.Gid,
		ModTime:  info.ModTime(),
		AccTime:  time.Now(),
		Segments: segments,
	}
	close(tasks)

	wp.Run(context.Background(), tasks, errs)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify data at offset 1MB.
	dstData, err := os.ReadFile(dstFile)
	require.NoError(t, err)
	assert.Len(t, dstData, int(fileSize))
	assert.Equal(t, data, dstData[1024*1024:1024*1024+4096])
}

func TestWorker_DirectoryCreation(t *testing.T) {
	dir := t.TempDir()
	dstRoot := filepath.Join(dir, "dst")
	dst := filepath.Join(dstRoot, "sub1", "sub2")

	wp, s := newTestWorkerPool(t, dstRoot)
	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)
	tasks <- FileTask{
		DstPath: dst,
		Type:    Dir,
		Mode:    uint32(os.ModeDir | 0755),
	}
	close(tasks)

	wp.Run(context.Background(), tasks, errs)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	info, err := os.Stat(dst)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	assert.Equal(t, int64(1), s.Snapshot().DirsCreated)
}

func TestWorker_SymlinkCreation(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(dst, 0755))

	linkPath := filepath.Join(dst, "link")

	wp, s := newTestWorkerPool(t, dst)
	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)
	tasks <- FileTask{
		DstPath:    linkPath,
		Type:       Symlink,
		LinkTarget: "/some/target",
	}
	close(tasks)

	wp.Run(context.Background(), tasks, errs)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	target, err := os.Readlink(linkPath)
	require.NoError(t, err)
	assert.Equal(t, "/some/target", target)
	assert.Equal(t, int64(1), s.Snapshot().FilesCopied)
}

func TestWorker_HardlinkCreation(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))

	// Create original file at destination (simulating it was already copied).
	data := []byte("hardlink data")
	dstOriginal := filepath.Join(dst, "original.txt")
	require.NoError(t, os.WriteFile(dstOriginal, data, 0644))

	srcOriginal := filepath.Join(src, "original.txt")
	srcHardlink := filepath.Join(src, "hardlink.txt")
	dstHardlink := filepath.Join(dst, "hardlink.txt")

	wp, s := newTestWorkerPool(t, dst)
	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)
	tasks <- FileTask{
		SrcPath:    srcHardlink,
		DstPath:    dstHardlink,
		Type:       Hardlink,
		LinkTarget: srcOriginal,
	}
	close(tasks)

	wp.Run(context.Background(), tasks, errs)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify hardlink exists and has same content.
	got, err := os.ReadFile(dstHardlink)
	require.NoError(t, err)
	assert.Equal(t, data, got)

	// Verify they share the same inode.
	origStat, err := os.Stat(dstOriginal)
	require.NoError(t, err)
	linkStat, err := os.Stat(dstHardlink)
	require.NoError(t, err)
	assert.Equal(t,
		origStat.Sys().(*syscall.Stat_t).Ino,
		linkStat.Sys().(*syscall.Stat_t).Ino,
	)
	assert.Equal(t, int64(1), s.Snapshot().HardlinksCreated)
}

func TestWorker_DryRun(t *testing.T) {
	dir := t.TempDir()

	wp, s := newTestWorkerPool(t, dir, func(cfg *WorkerConfig) {
		cfg.DryRun = true
	})

	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)
	tasks <- FileTask{
		SrcPath: filepath.Join(dir, "nonexistent"),
		DstPath: filepath.Join(dir, "dst"),
		Type:    Regular,
		Size:    100,
	}
	close(tasks)

	wp.Run(context.Background(), tasks, errs)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	snap := s.Snapshot()
	assert.Equal(t, int64(1), snap.FilesScanned)
	assert.Equal(t, int64(0), snap.FilesCopied) // dry run, nothing actually copied
}

func TestWorker_MetadataPreservation(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))

	srcFile := filepath.Join(src, "file.txt")
	require.NoError(t, os.WriteFile(srcFile, []byte("metadata test"), 0640))

	modTime := time.Date(2020, 6, 15, 12, 0, 0, 0, time.UTC)
	accTime := time.Date(2020, 6, 15, 13, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(srcFile, accTime, modTime))

	info, err := os.Stat(srcFile)
	require.NoError(t, err)
	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected *syscall.Stat_t")

	dstFile := filepath.Join(dst, "file.txt")

	wp, _ := newTestWorkerPool(t, dst, func(cfg *WorkerConfig) {
		cfg.PreserveMode = true
		cfg.PreserveTimes = true
	})

	tasks := make(chan FileTask, 1)
	errs := make(chan error, 1)
	tasks <- FileTask{
		SrcPath: srcFile,
		DstPath: dstFile,
		Type:    Regular,
		Size:    info.Size(),
		Mode:    uint32(info.Mode()),
		UID:     stat.Uid,
		GID:     stat.Gid,
		ModTime: modTime,
		AccTime: accTime,
	}
	close(tasks)

	wp.Run(context.Background(), tasks, errs)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	dstInfo, err := os.Stat(dstFile)
	require.NoError(t, err)

	// Check mode.
	assert.Equal(t, os.FileMode(0640), dstInfo.Mode().Perm())

	// Check mtime.
	assert.True(t, dstInfo.ModTime().Equal(modTime),
		"mtime: got %v, want %v", dstInfo.ModTime(), modTime)
}
