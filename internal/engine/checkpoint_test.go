package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpoint_OpenClose(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)

	cp, err := OpenCheckpoint("/src", "/dst")
	require.NoError(t, err)
	require.NotNil(t, cp)

	assert.FileExists(t, cp.Path())
	require.NoError(t, cp.Close())
}

func TestCheckpoint_MarkAndCheck(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)

	cp, err := OpenCheckpoint("/src", "/dst")
	require.NoError(t, err)
	defer cp.Close()

	// Not yet completed.
	assert.False(t, cp.IsCompleted("file.txt", 100, 12345))

	// Mark completed.
	require.NoError(t, cp.MarkCompleted("file.txt", 100, "abc123", 12345))
	require.NoError(t, cp.Flush())

	// Now it should be completed.
	assert.True(t, cp.IsCompleted("file.txt", 100, 12345))

	// Wrong size — not completed.
	assert.False(t, cp.IsCompleted("file.txt", 200, 12345))

	// Wrong mtime — not completed.
	assert.False(t, cp.IsCompleted("file.txt", 100, 99999))

	// Different path — not completed.
	assert.False(t, cp.IsCompleted("other.txt", 100, 12345))
}

func TestCheckpoint_BatchFlush(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)

	cp, err := OpenCheckpoint("/src", "/dst")
	require.NoError(t, err)
	defer cp.Close()

	// Add 150 entries — should auto-flush at 100.
	for i := range 150 {
		require.NoError(t, cp.MarkCompleted(
			filepath.Join("dir", fmt.Sprintf("file_%d.txt", i)),
			int64(i*100),
			"hash",
			int64(i*1000),
		))
	}

	require.NoError(t, cp.Flush())

	// Spot check first and last entries.
	assert.True(t, cp.IsCompleted("dir/file_0.txt", 0, 0))
	assert.True(t, cp.IsCompleted("dir/file_149.txt", 14900, 149000))
}

func TestCheckpoint_JobIDDeterminism(t *testing.T) {
	id1 := checkpointJobID("/src/a", "/dst/b")
	id2 := checkpointJobID("/src/a", "/dst/b")
	id3 := checkpointJobID("/src/a", "/dst/c")

	assert.Equal(t, id1, id2, "same inputs should produce same job ID")
	assert.NotEqual(t, id1, id3, "different inputs should produce different job IDs")
}

func TestCheckpoint_MetaValidation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)

	// Open with one pair.
	cp, err := OpenCheckpoint("/src/a", "/dst/b")
	require.NoError(t, err)
	require.NoError(t, cp.Close())

	// Reopen with same pair — should work.
	cp, err = OpenCheckpoint("/src/a", "/dst/b")
	require.NoError(t, err)
	require.NoError(t, cp.Close())
}

func TestCheckpoint_Remove(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)

	cp, err := OpenCheckpoint("/src", "/dst")
	require.NoError(t, err)

	dbPath := cp.Path()
	require.NoError(t, cp.Close())
	assert.FileExists(t, dbPath)

	require.NoError(t, os.Remove(dbPath))
	_, err = os.Stat(dbPath)
	assert.True(t, os.IsNotExist(err))
}

func TestCheckpoint_Resume(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)

	// First session: mark a file completed.
	cp, err := OpenCheckpoint("/src", "/dst")
	require.NoError(t, err)
	require.NoError(t, cp.MarkCompleted("done.txt", 500, "hash1", 99999))
	require.NoError(t, cp.Close())

	// Second session: should find the previously completed file.
	cp, err = OpenCheckpoint("/src", "/dst")
	require.NoError(t, err)
	defer cp.Close()

	assert.True(t, cp.IsCompleted("done.txt", 500, 99999))
	assert.False(t, cp.IsCompleted("new.txt", 100, 12345))
}
