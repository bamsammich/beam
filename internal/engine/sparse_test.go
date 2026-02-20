package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestDetectSparseSegments_NonSparse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "regular")

	data := make([]byte, 4096)
	for i := range data {
		data[i] = 'A'
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	fd, err := os.Open(path)
	require.NoError(t, err)
	defer fd.Close()

	segments, err := DetectSparseSegments(fd, int64(len(data)))
	require.NoError(t, err)

	// Should be a single data segment.
	require.GreaterOrEqual(t, len(segments), 1)
	totalData := int64(0)
	for _, seg := range segments {
		if seg.IsData {
			totalData += seg.Length
		}
	}
	assert.Equal(t, int64(len(data)), totalData)
}

func TestDetectSparseSegments_Sparse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sparse")

	// Create a sparse file: 1MB hole, then 4KB data.
	fd, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)

	// Truncate to 1MB + 4KB to create initial hole.
	fileSize := int64(1024*1024 + 4096)
	require.NoError(t, fd.Truncate(fileSize))

	// Write data at offset 1MB.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 'B'
	}
	_, err = unix.Pwrite(int(fd.Fd()), data, 1024*1024)
	require.NoError(t, err)
	fd.Close()

	// Re-open read-only for detection.
	rfd, err := os.Open(path)
	require.NoError(t, err)
	defer rfd.Close()

	segments, err := DetectSparseSegments(rfd, fileSize)
	require.NoError(t, err)

	// Should have at least a hole and a data segment.
	hasHole := false
	hasData := false
	for _, seg := range segments {
		if seg.IsData {
			hasData = true
		} else {
			hasHole = true
		}
	}
	assert.True(t, hasData, "expected at least one data segment")
	assert.True(t, hasHole, "expected at least one hole segment")
}

func TestDetectSparseSegments_AllHole(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "allhole")

	// Create file that is entirely a hole.
	fd, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	require.NoError(t, fd.Truncate(1024*1024))
	fd.Close()

	rfd, err := os.Open(path)
	require.NoError(t, err)
	defer rfd.Close()

	segments, err := DetectSparseSegments(rfd, 1024*1024)
	require.NoError(t, err)

	// Should be all hole segments (no data).
	totalData := int64(0)
	for _, seg := range segments {
		if seg.IsData {
			totalData += seg.Length
		}
	}
	assert.Equal(t, int64(0), totalData)
}

func TestDetectSparseSegments_Empty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty")

	require.NoError(t, os.WriteFile(path, nil, 0644))

	fd, err := os.Open(path)
	require.NoError(t, err)
	defer fd.Close()

	segments, err := DetectSparseSegments(fd, 0)
	require.NoError(t, err)
	assert.Nil(t, segments)
}
