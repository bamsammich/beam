package transport_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub", "deep"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("hello"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sub", "nested.txt"), []byte("nested content"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sub", "deep", "deep.txt"), []byte("deep"), 0644))
	require.NoError(t, os.Symlink("nested.txt", filepath.Join(root, "sub", "link")))

	return root
}

func TestLocalReadEndpoint_Walk(t *testing.T) {
	t.Parallel()
	root := setupTestTree(t)
	ep := transport.NewLocalReadEndpoint(root)

	var entries []transport.FileEntry
	err := ep.Walk(func(entry transport.FileEntry) error {
		entries = append(entries, entry)
		return nil
	})
	require.NoError(t, err)

	// Should find: file.txt, sub/, sub/nested.txt, sub/link, sub/deep/, sub/deep/deep.txt
	relPaths := make(map[string]bool)
	for _, e := range entries {
		relPaths[e.RelPath] = true
	}

	assert.True(t, relPaths["file.txt"], "should find file.txt")
	assert.True(t, relPaths["sub"], "should find sub/")
	assert.True(t, relPaths[filepath.Join("sub", "nested.txt")], "should find sub/nested.txt")
	assert.True(t, relPaths[filepath.Join("sub", "deep")], "should find sub/deep/")
	assert.True(t, relPaths[filepath.Join("sub", "deep", "deep.txt")], "should find sub/deep/deep.txt")
}

func TestLocalReadEndpoint_Stat(t *testing.T) {
	t.Parallel()
	root := setupTestTree(t)
	ep := transport.NewLocalReadEndpoint(root)

	entry, err := ep.Stat("file.txt")
	require.NoError(t, err)
	assert.Equal(t, "file.txt", entry.RelPath)
	assert.Equal(t, int64(5), entry.Size)
	assert.False(t, entry.IsDir)
	assert.False(t, entry.IsSymlink)

	// Directory.
	entry, err = ep.Stat("sub")
	require.NoError(t, err)
	assert.True(t, entry.IsDir)

	// Nonexistent.
	_, err = ep.Stat("nonexistent")
	assert.Error(t, err)
}

func TestLocalReadEndpoint_ReadDir(t *testing.T) {
	t.Parallel()
	root := setupTestTree(t)
	ep := transport.NewLocalReadEndpoint(root)

	entries, err := ep.ReadDir("sub")
	require.NoError(t, err)

	names := make(map[string]bool)
	for _, e := range entries {
		names[filepath.Base(e.RelPath)] = true
	}
	assert.True(t, names["nested.txt"])
	assert.True(t, names["deep"])
	assert.True(t, names["link"])
}

func TestLocalReadEndpoint_OpenRead(t *testing.T) {
	t.Parallel()
	root := setupTestTree(t)
	ep := transport.NewLocalReadEndpoint(root)

	rc, err := ep.OpenRead("file.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), data)
}

func TestLocalReadEndpoint_Hash(t *testing.T) {
	t.Parallel()
	root := setupTestTree(t)
	ep := transport.NewLocalReadEndpoint(root)

	hash, err := ep.Hash("file.txt")
	require.NoError(t, err)
	assert.NotEmpty(t, hash)
	assert.Len(t, hash, 64) // BLAKE3 produces 32 bytes = 64 hex chars

	// Same content should produce same hash.
	hash2, err := ep.Hash("file.txt")
	require.NoError(t, err)
	assert.Equal(t, hash, hash2)
}

func TestLocalReadEndpoint_Caps(t *testing.T) {
	t.Parallel()
	ep := transport.NewLocalReadEndpoint("/tmp")
	caps := ep.Caps()
	assert.True(t, caps.SparseDetect)
	assert.True(t, caps.Hardlinks)
	assert.True(t, caps.Xattrs)
	assert.True(t, caps.AtomicRename)
	assert.True(t, caps.FastCopy)
	assert.True(t, caps.NativeHash)
}

func TestLocalWriteEndpoint_MkdirAll(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ep := transport.NewLocalWriteEndpoint(root)

	err := ep.MkdirAll(filepath.Join("a", "b", "c"), 0755)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(root, "a", "b", "c"))
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestLocalWriteEndpoint_CreateTemp_Rename(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ep := transport.NewLocalWriteEndpoint(root)

	wf, err := ep.CreateTemp("target.txt", 0644)
	require.NoError(t, err)

	_, err = wf.Write([]byte("temp content"))
	require.NoError(t, err)

	// Get the tmp file's relative path.
	tmpRel := wf.(*transport.TestableWriteFile).RelPath()
	require.NoError(t, wf.Close())

	// Rename tmp to target.
	err = ep.Rename(tmpRel, "target.txt")
	require.NoError(t, err)

	// Read the final file.
	data, err := os.ReadFile(filepath.Join(root, "target.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("temp content"), data)
}

func TestLocalWriteEndpoint_Remove(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "removeme.txt"), []byte("bye"), 0644))

	ep := transport.NewLocalWriteEndpoint(root)
	err := ep.Remove("removeme.txt")
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(root, "removeme.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestLocalWriteEndpoint_RemoveAll(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dir", "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dir", "sub", "f.txt"), []byte("x"), 0644))

	ep := transport.NewLocalWriteEndpoint(root)
	err := ep.RemoveAll("dir")
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(root, "dir"))
	assert.True(t, os.IsNotExist(err))
}

func TestLocalWriteEndpoint_Symlink(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "target.txt"), []byte("data"), 0644))

	ep := transport.NewLocalWriteEndpoint(root)
	err := ep.Symlink("target.txt", "link.txt")
	require.NoError(t, err)

	target, err := os.Readlink(filepath.Join(root, "link.txt"))
	require.NoError(t, err)
	assert.Equal(t, "target.txt", target)
}

func TestLocalWriteEndpoint_Link(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "original.txt"), []byte("data"), 0644))

	ep := transport.NewLocalWriteEndpoint(root)
	err := ep.Link("original.txt", "hardlink.txt")
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(root, "hardlink.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), data)

	// Verify they share the same inode.
	orig, err := os.Stat(filepath.Join(root, "original.txt"))
	require.NoError(t, err)
	link, err := os.Stat(filepath.Join(root, "hardlink.txt"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(orig, link))
}

func TestLocalWriteEndpoint_Walk(t *testing.T) {
	t.Parallel()
	root := setupTestTree(t)
	ep := transport.NewLocalWriteEndpoint(root)

	var entries []transport.FileEntry
	err := ep.Walk(func(entry transport.FileEntry) error {
		entries = append(entries, entry)
		return nil
	})
	require.NoError(t, err)
	assert.NotEmpty(t, entries)
}

func TestLocalWriteEndpoint_Hash(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "test.txt"), []byte("hash me"), 0644))

	readEP := transport.NewLocalReadEndpoint(root)
	writeEP := transport.NewLocalWriteEndpoint(root)

	hash1, err := readEP.Hash("test.txt")
	require.NoError(t, err)
	hash2, err := writeEP.Hash("test.txt")
	require.NoError(t, err)
	assert.Equal(t, hash1, hash2)
}

func TestLocalFile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ep := transport.NewLocalWriteEndpoint(root)

	wf, err := ep.CreateTemp("test.txt", 0644)
	require.NoError(t, err)
	defer wf.Close()

	f := transport.LocalFile(wf)
	assert.NotNil(t, f, "LocalFile should return non-nil for local WriteFile")
	assert.NotZero(t, f.Fd(), "should have a valid fd")
}
