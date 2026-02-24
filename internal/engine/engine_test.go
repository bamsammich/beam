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

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/transport"
)

// localConns returns a pair of LocalTransports for tests.
//
//nolint:ireturn // test helper
func localConns() (transport.Transport, transport.Transport) {
	return transport.NewLocalTransport(), transport.NewLocalTransport()
}

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
	require.NoError(
		t,
		os.WriteFile(filepath.Join(src, "sub", "deep", "nested.txt"), []byte("nested"), 0644),
	)
	require.NoError(t, os.Symlink("nested.txt", filepath.Join(src, "sub", "deep", "link")))

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Archive:      true,
		Workers:      4,
		Recursive:    true,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.NoError(t, result.Err)
	assert.Positive(t, result.Stats.FilesCopied)
	assert.Positive(t, result.Stats.DirsCreated)

	// Verify checksums.
	assert.Equal(
		t,
		hashFile(t, filepath.Join(src, "root.txt")),
		hashFile(t, filepath.Join(dst, "root.txt")),
	)
	assert.Equal(
		t,
		hashFile(t, filepath.Join(src, "sub", "big.bin")),
		hashFile(t, filepath.Join(dst, "sub", "big.bin")),
	)
	assert.Equal(
		t,
		hashFile(t, filepath.Join(src, "sub", "deep", "nested.txt")),
		hashFile(t, filepath.Join(dst, "sub", "deep", "nested.txt")),
	)

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

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src},
		Dst:          dst,
		Workers:      1,
		SrcTransport: srcConn,
		DstTransport: dstConn,
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

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src},
		Dst:          dstDir,
		Workers:      1,
		SrcTransport: srcConn,
		DstTransport: dstConn,
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

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src},
		Dst:          dst,
		Workers:      1,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "directory")
}

func TestEngine_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	for i := range 50 {
		data := make([]byte, 1024*1024) // 1 MiB each
		require.NoError(
			t,
			os.WriteFile(filepath.Join(src, filepath.Base(string(rune('A'+i)))+".bin"), data, 0644),
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	srcConn, dstConn := localConns()
	result := Run(ctx, Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Workers:      4,
		SrcTransport: srcConn,
		DstTransport: dstConn,
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
	require.NoError(
		t,
		os.WriteFile(filepath.Join(src, "sub", "nested.txt"), []byte("nested"), 0644),
	)

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		DryRun:       true,
		Workers:      2,
		SrcTransport: srcConn,
		DstTransport: dstConn,
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
	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{"/nonexistent/path"},
		Dst:          "/tmp/dst",
		Workers:      1,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})
	assert.Error(t, result.Err)
}

func TestEngine_EventSequence(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "file.txt"), []byte("data"), 0644))
	require.NoError(
		t,
		os.WriteFile(filepath.Join(src, "sub", "nested.txt"), []byte("nested"), 0644),
	)

	events := make(chan event.Event, 256)

	var collected []event.Event
	done := make(chan struct{})
	go func() {
		for ev := range events {
			collected = append(collected, ev)
		}
		close(done)
	}()

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		Events:       events,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	close(events)
	<-done

	require.NoError(t, result.Err)

	// Should have ScanComplete, ScanStarted, FileStarted, FileCompleted, DirCreated events.
	typeSet := make(map[event.Type]bool)
	for _, ev := range collected {
		typeSet[ev.Type] = true
	}

	assert.True(t, typeSet[event.ScanComplete], "expected ScanComplete event")
	assert.True(t, typeSet[event.ScanStarted], "expected ScanStarted event")
	assert.True(t, typeSet[event.FileStarted], "expected FileStarted event")
	assert.True(t, typeSet[event.FileCompleted], "expected FileCompleted event")
	assert.True(t, typeSet[event.DirCreated], "expected DirCreated event")

	// Prescan emits ScanComplete first (with totals), then scanner emits ScanStarted.
	assert.Equal(t, event.ScanComplete, collected[0].Type)
}

func TestEngine_WithFilter(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "keep.txt"), []byte("keep"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "skip.log"), []byte("skip"), 0644))

	chain := filter.NewChain()
	require.NoError(t, chain.AddExclude("*.log"))

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		Filter:       chain,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.NoError(t, result.Err)

	// keep.txt should exist.
	_, err := os.Stat(filepath.Join(dst, "keep.txt"))
	require.NoError(t, err)

	// skip.log should NOT exist.
	_, err = os.Stat(filepath.Join(dst, "skip.log"))
	assert.True(t, os.IsNotExist(err))
}

func TestEngine_DeleteExtraneous(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.Mkdir(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "keep.txt"), []byte("keep"), 0644))

	// Pre-populate destination with extra file.
	require.NoError(t, os.MkdirAll(dst, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "keep.txt"), []byte("old"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "extra.txt"), []byte("extra"), 0644))

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		Delete:       true,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.NoError(t, result.Err)

	// keep.txt should have been overwritten.
	data, err := os.ReadFile(filepath.Join(dst, "keep.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("keep"), data)

	// extra.txt should be deleted.
	_, err = os.Stat(filepath.Join(dst, "extra.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestEngine_SkipUnchanged(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("aaa"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "b.txt"), []byte("bbb"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "c.txt"), []byte("ccc"), 0644))

	// First run: copy all files.
	srcConn1, dstConn1 := localConns()
	result1 := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Archive:      true,
		Workers:      2,
		SrcTransport: srcConn1,
		DstTransport: dstConn1,
	})
	require.NoError(t, result1.Err)
	assert.Equal(t, int64(3), result1.Stats.FilesCopied)

	// Second run: destination matches source (size + mtime), all skipped.
	srcConn2, dstConn2 := localConns()
	result2 := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Archive:      true,
		Workers:      2,
		SrcTransport: srcConn2,
		DstTransport: dstConn2,
	})
	require.NoError(t, result2.Err)
	assert.Equal(t, int64(0), result2.Stats.FilesCopied)
}

func TestEngine_SkipUnchanged_NoArchive(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("aaa"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "b.txt"), []byte("bbb"), 0644))

	// First run without archive mode.
	srcConn1, dstConn1 := localConns()
	result1 := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		SrcTransport: srcConn1,
		DstTransport: dstConn1,
	})
	require.NoError(t, result1.Err)
	assert.Equal(t, int64(2), result1.Stats.FilesCopied)

	// Second run: mtime is always preserved, so skip detection works
	// even without archive mode.
	srcConn2, dstConn2 := localConns()
	result2 := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		SrcTransport: srcConn2,
		DstTransport: dstConn2,
	})
	require.NoError(t, result2.Err)
	assert.Equal(t, int64(0), result2.Stats.FilesCopied)
}

func TestEngine_RecopiesDeletedFiles(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("aaa"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "b.txt"), []byte("bbb"), 0644))

	// First run.
	srcConn1, dstConn1 := localConns()
	result1 := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Archive:      true,
		Workers:      1,
		SrcTransport: srcConn1,
		DstTransport: dstConn1,
	})
	require.NoError(t, result1.Err)
	assert.Equal(t, int64(2), result1.Stats.FilesCopied)

	// Delete a destination file.
	require.NoError(t, os.Remove(filepath.Join(dst, "a.txt")))

	// Second run: should re-copy the deleted file.
	srcConn2, dstConn2 := localConns()
	result2 := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Archive:      true,
		Workers:      1,
		SrcTransport: srcConn2,
		DstTransport: dstConn2,
	})
	require.NoError(t, result2.Err)
	assert.Equal(t, int64(1), result2.Stats.FilesCopied)

	// Verify the file was restored.
	data, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("aaa"), data)
}

func TestEngine_MultiSourceFiles(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(dst, 0755))

	// Create multiple source files in different locations.
	file1 := filepath.Join(dir, "file1.txt")
	file2 := filepath.Join(dir, "file2.txt")
	require.NoError(t, os.WriteFile(file1, []byte("content1"), 0644))
	require.NoError(t, os.WriteFile(file2, []byte("content2"), 0644))

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{file1, file2},
		Dst:          dst,
		Workers:      2,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.NoError(t, result.Err)

	// Both files should exist in dst.
	got1, err := os.ReadFile(filepath.Join(dst, "file1.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("content1"), got1)

	got2, err := os.ReadFile(filepath.Join(dst, "file2.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("content2"), got2)
}

func TestEngine_MultiSourceDirs(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "dst")

	// Create two source directories (no trailing slash = copy dir itself).
	srcA := filepath.Join(dir, "srcA")
	srcB := filepath.Join(dir, "srcB")
	require.NoError(t, os.MkdirAll(srcA, 0755))
	require.NoError(t, os.MkdirAll(srcB, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcA, "a.txt"), []byte("aaa"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(srcB, "b.txt"), []byte("bbb"), 0644))

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{srcA, srcB},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.NoError(t, result.Err)

	// Each dir should be created as a subdirectory of dst.
	gotA, err := os.ReadFile(filepath.Join(dst, "srcA", "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("aaa"), gotA)

	gotB, err := os.ReadFile(filepath.Join(dst, "srcB", "b.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("bbb"), gotB)
}

func TestEngine_MultiSourceMixed(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(dst, 0755))

	// Create a source directory and a source file.
	srcDir := filepath.Join(dir, "srcdir")
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "nested.txt"), []byte("nested"), 0644))

	srcFile := filepath.Join(dir, "loose.txt")
	require.NoError(t, os.WriteFile(srcFile, []byte("loose"), 0644))

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{srcDir, srcFile},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.NoError(t, result.Err)

	// Directory source (no trailing slash) → dst/srcdir/nested.txt
	gotNested, err := os.ReadFile(filepath.Join(dst, "srcdir", "nested.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("nested"), gotNested)

	// File source → dst/loose.txt
	gotLoose, err := os.ReadFile(filepath.Join(dst, "loose.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("loose"), gotLoose)
}

func TestEngine_TrailingSlash_CopyContents(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "root.txt"), []byte("root"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "child.txt"), []byte("child"), 0644))

	// Trailing slash: copy contents of src into dst directly.
	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src + "/"},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.NoError(t, result.Err)

	// Files should be directly in dst, not dst/src/.
	gotRoot, err := os.ReadFile(filepath.Join(dst, "root.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("root"), gotRoot)

	gotChild, err := os.ReadFile(filepath.Join(dst, "sub", "child.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("child"), gotChild)

	// dst/src/ should NOT exist.
	_, err = os.Stat(filepath.Join(dst, "src"))
	assert.True(t, os.IsNotExist(err), "dst/src/ should not exist with trailing slash")
}

func TestEngine_NoTrailingSlash_CopyDir(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "root.txt"), []byte("root"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "child.txt"), []byte("child"), 0644))

	// No trailing slash: copy dir itself into dst, creating dst/src/.
	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.NoError(t, result.Err)

	// Files should be under dst/src/.
	gotRoot, err := os.ReadFile(filepath.Join(dst, "src", "root.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("root"), gotRoot)

	gotChild, err := os.ReadFile(filepath.Join(dst, "src", "sub", "child.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("child"), gotChild)
}

func TestEngine_MultiSourceDstMustBeDir(t *testing.T) {
	dir := t.TempDir()

	file1 := filepath.Join(dir, "file1.txt")
	file2 := filepath.Join(dir, "file2.txt")
	dstFile := filepath.Join(dir, "dst.txt")

	require.NoError(t, os.WriteFile(file1, []byte("a"), 0644))
	require.NoError(t, os.WriteFile(file2, []byte("b"), 0644))
	require.NoError(t, os.WriteFile(dstFile, []byte("existing"), 0644))

	// Multiple sources to an existing file destination should error.
	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{file1, file2},
		Dst:          dstFile,
		Workers:      1,
		SrcTransport: srcConn,
		DstTransport: dstConn,
	})

	require.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "not a directory")
}
