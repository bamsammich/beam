package engine

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
)

func hashFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	h := blake3.Sum256(data)
	return h[:]
}

func TestEngine_CopyTree(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	// Build test tree.
	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub", "deep"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "root.txt"), []byte("root file"), 0644))

	bigData := make([]byte, 2*1024*1024)
	_, err := rand.Read(bigData)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "big.bin"), bigData, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "deep", "nested.txt"), []byte("nested"), 0644))
	require.NoError(t, os.Symlink("nested.txt", filepath.Join(src, "sub", "deep", "link")))

	result := Run(context.Background(), Config{
		Src:       src,
		Dst:       dst,
		Archive:   true,
		Workers:   4,
		Recursive: true,
	})

	require.NoError(t, result.Err)
	assert.Greater(t, result.Stats.FilesCopied, int64(0))
	assert.Greater(t, result.Stats.DirsCreated, int64(0))

	// Verify checksums.
	assert.Equal(t, hashFile(t, filepath.Join(src, "root.txt")), hashFile(t, filepath.Join(dst, "root.txt")))
	assert.Equal(t, hashFile(t, filepath.Join(src, "sub", "big.bin")), hashFile(t, filepath.Join(dst, "sub", "big.bin")))
	assert.Equal(t, hashFile(t, filepath.Join(src, "sub", "deep", "nested.txt")), hashFile(t, filepath.Join(dst, "sub", "deep", "nested.txt")))

	// Verify symlink.
	target, err := os.Readlink(filepath.Join(dst, "sub", "deep", "link"))
	require.NoError(t, err)
	assert.Equal(t, "nested.txt", target)
}

func TestEngine_SingleFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	data := []byte("single file copy")
	require.NoError(t, os.WriteFile(src, data, 0644))

	result := Run(context.Background(), Config{
		Src:     src,
		Dst:     dst,
		Workers: 1,
	})

	require.NoError(t, result.Err)
	assert.Equal(t, int64(1), result.Stats.FilesCopied)
	assert.Equal(t, hashFile(t, src), hashFile(t, dst))
}

func TestEngine_SingleFileIntoDirDst(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dstDir := filepath.Join(dir, "dstdir")
	require.NoError(t, os.MkdirAll(dstDir, 0755))

	data := []byte("single file into dir")
	require.NoError(t, os.WriteFile(src, data, 0644))

	result := Run(context.Background(), Config{
		Src:     src,
		Dst:     dstDir,
		Workers: 1,
	})

	require.NoError(t, result.Err)

	// Should have copied into dstDir/src.txt.
	got, err := os.ReadFile(filepath.Join(dstDir, "src.txt"))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestEngine_DirWithoutRecursive(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(src, 0755))

	result := Run(context.Background(), Config{
		Src:     src,
		Dst:     dst,
		Workers: 1,
	})

	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "directory")
}

func TestEngine_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	for i := range 50 {
		data := make([]byte, 1024*1024) // 1 MiB each
		require.NoError(t, os.WriteFile(filepath.Join(src, filepath.Base(string(rune('A'+i)))+".bin"), data, 0644))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	result := Run(ctx, Config{
		Src:       src,
		Dst:       dst,
		Recursive: true,
		Workers:   4,
	})

	// With immediate cancel, might get an error or partial copy.
	t.Logf("result: copied=%d, err=%v", result.Stats.FilesCopied, result.Err)
}

func TestEngine_DryRun(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "file.txt"), []byte("data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "nested.txt"), []byte("nested"), 0644))

	result := Run(context.Background(), Config{
		Src:       src,
		Dst:       dst,
		Recursive: true,
		DryRun:    true,
		Workers:   2,
	})

	require.NoError(t, result.Err)

	// Destination should only have the root dir created (needed for scanner),
	// but no files should have been copied.
	assert.Equal(t, int64(0), result.Stats.FilesCopied)

	// Verify no files were actually written inside dst.
	_, err := os.Stat(filepath.Join(dst, "file.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestEngine_SourceNotExist(t *testing.T) {
	result := Run(context.Background(), Config{
		Src:     "/nonexistent/path",
		Dst:     "/tmp/dst",
		Workers: 1,
	})
	assert.Error(t, result.Err)
}
