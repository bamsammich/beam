package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanner_FlatDir(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("A"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "b.txt"), []byte("B"), 0644))

	cfg := ScannerConfig{
		SrcRoot: src,
		DstRoot: dst,
		Workers: 2,
	}

	scanner := NewScanner(cfg)
	tasks, errs := scanner.Scan(context.Background())

	var taskList []FileTask
	var errList []error

	done := make(chan struct{})
	go func() {
		for task := range tasks {
			taskList = append(taskList, task)
		}
		close(done)
	}()

	for err := range errs {
		errList = append(errList, err)
	}
	<-done

	require.Empty(t, errList)
	require.Len(t, taskList, 2) // Two regular files (root dir is not emitted).

	fileCount := 0
	for _, task := range taskList {
		if task.Type == Regular {
			fileCount++
		}
	}
	assert.Equal(t, 2, fileCount)
}

func TestScanner_NestedDirs(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub1", "sub2"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "root.txt"), []byte("root"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub1", "s1.txt"), []byte("s1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub1", "sub2", "s2.txt"), []byte("s2"), 0644))

	cfg := ScannerConfig{
		SrcRoot: src,
		DstRoot: dst,
		Workers: 2,
	}

	scanner := NewScanner(cfg)
	tasks, errs := scanner.Scan(context.Background())

	var taskList []FileTask
	var errList []error

	done := make(chan struct{})
	go func() {
		for task := range tasks {
			taskList = append(taskList, task)
		}
		close(done)
	}()

	for err := range errs {
		errList = append(errList, err)
	}
	<-done

	require.Empty(t, errList)

	// Should have: 2 dirs (sub1, sub2) + 3 files.
	dirCount := 0
	fileCount := 0
	for _, task := range taskList {
		switch task.Type {
		case Dir:
			dirCount++
		case Regular:
			fileCount++
		}
	}
	assert.Equal(t, 2, dirCount)
	assert.Equal(t, 3, fileCount)

	// Verify directory ordering: directories should appear before their contents.
	dirsSeen := make(map[string]bool)
	for _, task := range taskList {
		if task.Type == Dir {
			dirsSeen[task.SrcPath] = true
		} else if task.Type == Regular {
			parent := filepath.Dir(task.SrcPath)
			if parent != src {
				assert.True(t, dirsSeen[parent], "parent dir %s should be emitted before file %s", parent, task.SrcPath)
			}
		}
	}
}

func TestScanner_Symlink(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "target.txt"), []byte("target"), 0644))
	require.NoError(t, os.Symlink("target.txt", filepath.Join(src, "link")))

	cfg := ScannerConfig{
		SrcRoot: src,
		DstRoot: dst,
		Workers: 1,
	}

	scanner := NewScanner(cfg)
	tasks, errs := scanner.Scan(context.Background())

	var taskList []FileTask
	var errList []error

	done := make(chan struct{})
	go func() {
		for task := range tasks {
			taskList = append(taskList, task)
		}
		close(done)
	}()

	for err := range errs {
		errList = append(errList, err)
	}
	<-done

	require.Empty(t, errList)

	symlinkFound := false
	for _, task := range taskList {
		if task.Type == Symlink {
			symlinkFound = true
			assert.Equal(t, "target.txt", task.LinkTarget)
		}
	}
	assert.True(t, symlinkFound)
}

func TestScanner_Hardlink(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))
	original := filepath.Join(src, "original.txt")
	hardlink := filepath.Join(src, "hardlink.txt")

	require.NoError(t, os.WriteFile(original, []byte("content"), 0644))
	require.NoError(t, os.Link(original, hardlink))

	cfg := ScannerConfig{
		SrcRoot: src,
		DstRoot: dst,
		Workers: 1,
	}

	scanner := NewScanner(cfg)
	tasks, errs := scanner.Scan(context.Background())

	var taskList []FileTask
	var errList []error

	done := make(chan struct{})
	go func() {
		for task := range tasks {
			taskList = append(taskList, task)
		}
		close(done)
	}()

	for err := range errs {
		errList = append(errList, err)
	}
	<-done

	require.Empty(t, errList)

	regularCount := 0
	hardlinkCount := 0
	for _, task := range taskList {
		if task.Type == Regular {
			regularCount++
		} else if task.Type == Hardlink {
			hardlinkCount++
		}
	}
	assert.Equal(t, 1, regularCount, "one regular file")
	assert.Equal(t, 1, hardlinkCount, "one hardlink")
}

func TestScanner_LargeFileChunking(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))
	largefile := filepath.Join(src, "large.bin")
	require.NoError(t, os.WriteFile(largefile, make([]byte, 10*1024*1024), 0644))

	cfg := ScannerConfig{
		SrcRoot:        src,
		DstRoot:        dst,
		Workers:        1,
		ChunkThreshold: 4 * 1024 * 1024, // 4 MiB
	}

	scanner := NewScanner(cfg)
	tasks, errs := scanner.Scan(context.Background())

	var taskList []FileTask
	var errList []error

	done := make(chan struct{})
	go func() {
		for task := range tasks {
			taskList = append(taskList, task)
		}
		close(done)
	}()

	for err := range errs {
		errList = append(errList, err)
	}
	<-done

	require.Empty(t, errList)
	require.Len(t, taskList, 1)

	task := taskList[0]
	assert.Equal(t, Regular, task.Type)
	assert.Greater(t, len(task.Chunks), 1, "large file should be chunked")

	totalChunkSize := int64(0)
	for _, chunk := range task.Chunks {
		totalChunkSize += chunk.Length
	}
	assert.Equal(t, task.Size, totalChunkSize)
}

func TestScanner_SparseDetection(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))

	// Create a sparse file.
	sparsePath := filepath.Join(src, "sparse.bin")
	fd, err := os.OpenFile(sparsePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	require.NoError(t, fd.Truncate(10*1024*1024))
	fd.Close()

	cfg := ScannerConfig{
		SrcRoot:      src,
		DstRoot:      dst,
		Workers:      1,
		SparseDetect: true,
	}

	scanner := NewScanner(cfg)
	tasks, errs := scanner.Scan(context.Background())

	var taskList []FileTask
	var errList []error

	done := make(chan struct{})
	go func() {
		for task := range tasks {
			taskList = append(taskList, task)
		}
		close(done)
	}()

	for err := range errs {
		errList = append(errList, err)
	}
	<-done

	require.Empty(t, errList)
	require.Len(t, taskList, 1)

	task := taskList[0]
	assert.NotNil(t, task.Segments)
}

func TestScanner_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))
	for i := range 100 {
		require.NoError(t, os.WriteFile(filepath.Join(src, fmt.Sprintf("file%d", i)), []byte("data"), 0644))
	}

	cfg := ScannerConfig{
		SrcRoot: src,
		DstRoot: dst,
		Workers: 2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	scanner := NewScanner(cfg)
	tasks, errs := scanner.Scan(ctx)

	taskCount := 0
	for range tasks {
		taskCount++
	}

	errCount := 0
	for range errs {
		errCount++
	}

	// With immediate cancel, we should get very few (possibly zero) tasks.
	t.Logf("got %d tasks with immediate cancel", taskCount)
}

func TestScanner_PermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root, cannot test permission denied")
	}

	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))
	subdir := filepath.Join(src, "forbidden")
	require.NoError(t, os.Mkdir(subdir, 0000))
	defer os.Chmod(subdir, 0755) // cleanup

	cfg := ScannerConfig{
		SrcRoot: src,
		DstRoot: dst,
		Workers: 1,
	}

	scanner := NewScanner(cfg)
	tasks, errs := scanner.Scan(context.Background())

	var taskList []FileTask
	var errList []error

	done := make(chan struct{})
	go func() {
		for task := range tasks {
			taskList = append(taskList, task)
		}
		close(done)
	}()

	for err := range errs {
		errList = append(errList, err)
	}
	<-done

	// Should get an error for the forbidden directory.
	assert.Greater(t, len(errList), 0)
}
