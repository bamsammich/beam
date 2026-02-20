package platform

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyFileBasic(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	data := []byte("hello, beam!")
	require.NoError(t, os.WriteFile(src, data, 0644))

	dstFd, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer dstFd.Close()

	result, err := CopyFile(CopyFileParams{
		SrcPath: src,
		DstFd:   dstFd,
		SrcSize: int64(len(data)),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), result.BytesWritten)

	dstFd.Close()
	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestCopyFileLarge(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	// 4 MiB — larger than the 1 MiB buffer.
	size := 4 * 1024 * 1024
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(src, data, 0644))

	dstFd, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer dstFd.Close()

	result, err := CopyFile(CopyFileParams{
		SrcPath: src,
		DstFd:   dstFd,
		SrcSize: int64(size),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(size), result.BytesWritten)

	dstFd.Close()
	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestCopyFileOffset(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	data := []byte("AAAA_BBBB_CCCC")
	require.NoError(t, os.WriteFile(src, data, 0644))

	dstFd, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer dstFd.Close()

	// Copy only "BBBB" (offset 5, length 4).
	result, err := CopyFile(CopyFileParams{
		SrcPath:   src,
		DstFd:     dstFd,
		SrcOffset: 5,
		Length:    4,
		SrcSize:   int64(len(data)),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.BytesWritten)

	dstFd.Close()
	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	// Data is written at offset 5 in destination as well (pwrite semantics).
	assert.Equal(t, []byte("BBBB"), got[5:9])
}

func TestCopyFileEmpty(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.WriteFile(src, nil, 0644))

	dstFd, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer dstFd.Close()

	result, err := CopyFile(CopyFileParams{
		SrcPath: src,
		DstFd:   dstFd,
		SrcSize: 0,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.BytesWritten)
}

func TestCopyReadWrite(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	data := []byte("read-write fallback test")
	require.NoError(t, os.WriteFile(src, data, 0644))

	dstFd, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer dstFd.Close()

	result, err := CopyReadWrite(CopyFileParams{
		SrcPath: src,
		DstFd:   dstFd,
		SrcSize: int64(len(data)),
	})
	require.NoError(t, err)
	assert.Equal(t, ReadWrite, result.Method)
	assert.Equal(t, int64(len(data)), result.BytesWritten)

	dstFd.Close()
	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestIOURingDetection(t *testing.T) {
	// Just verify the function doesn't panic.
	supported := KernelSupportsIOURing()
	t.Logf("io_uring supported: %v", supported)
}

func TestIOURingCopier(t *testing.T) {
	copier, err := NewIOURingCopier(64)
	if copier == nil {
		t.Skip("io_uring not available on this kernel")
	}
	require.NoError(t, err)
	defer copier.Close()

	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	data := make([]byte, 2*1024*1024)
	_, err = rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(src, data, 0644))

	dstFd, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer dstFd.Close()

	result, err := copier.CopyFile(CopyFileParams{
		SrcPath: src,
		DstFd:   dstFd,
		SrcSize: int64(len(data)),
	})
	require.NoError(t, err)
	assert.Equal(t, IOURing, result.Method)
	assert.Equal(t, int64(len(data)), result.BytesWritten)

	dstFd.Close()
	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestCopyMethodString(t *testing.T) {
	assert.Equal(t, "read_write", ReadWrite.String())
	assert.Equal(t, "copy_file_range", CopyFileRange.String())
	assert.Equal(t, "sendfile", Sendfile.String())
	assert.Equal(t, "io_uring", IOURing.String())
	assert.Equal(t, "clonefile", Clonefile.String())
	assert.Equal(t, "unknown", CopyMethod(99).String())
}
