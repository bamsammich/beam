package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteExtraneous_OverlappingTrees(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	// Source: has file1.txt and file2.txt.
	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "file1.txt"), []byte("1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "file2.txt"), []byte("2"), 0644))

	// Destination: has file1.txt, file2.txt, and extra.txt.
	require.NoError(t, os.MkdirAll(dst, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "file1.txt"), []byte("1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "file2.txt"), []byte("2"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "extra.txt"), []byte("extra"), 0644))

	deleted, err := DeleteExtraneous(context.Background(), DeleteConfig{
		SrcRoot: src,
		DstRoot: dst,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	// extra.txt should be gone.
	_, err = os.Stat(filepath.Join(dst, "extra.txt"))
	assert.True(t, os.IsNotExist(err))

	// Others should remain.
	_, err = os.Stat(filepath.Join(dst, "file1.txt"))
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(dst, "file2.txt"))
	assert.NoError(t, err)
}

func TestDeleteExtraneous_NonOverlapping(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "keep.txt"), []byte("k"), 0644))

	require.NoError(t, os.MkdirAll(dst, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "delete_me.txt"), []byte("d"), 0644))

	deleted, err := DeleteExtraneous(context.Background(), DeleteConfig{
		SrcRoot: src,
		DstRoot: dst,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	_, err = os.Stat(filepath.Join(dst, "delete_me.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestDeleteExtraneous_DryRun(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "extra.txt"), []byte("extra"), 0644))

	deleted, err := DeleteExtraneous(context.Background(), DeleteConfig{
		SrcRoot: src,
		DstRoot: dst,
		DryRun:  true,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	// File should still exist in dry-run mode.
	_, err = os.Stat(filepath.Join(dst, "extra.txt"))
	assert.NoError(t, err)
}

func TestDeleteExtraneous_FilterInteraction(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "keep.txt"), []byte("k"), 0644))

	require.NoError(t, os.MkdirAll(dst, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "keep.txt"), []byte("k"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "filtered.log"), []byte("log"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "extra.txt"), []byte("e"), 0644))

	// Exclude *.log — filtered files should not be deleted.
	chain := filter.NewChain()
	require.NoError(t, chain.AddExclude("*.log"))

	deleted, err := DeleteExtraneous(context.Background(), DeleteConfig{
		SrcRoot: src,
		DstRoot: dst,
		Filter:  chain,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, deleted) // only extra.txt

	// filtered.log should remain (excluded from filter = don't touch).
	_, err = os.Stat(filepath.Join(dst, "filtered.log"))
	assert.NoError(t, err)

	// extra.txt should be deleted.
	_, err = os.Stat(filepath.Join(dst, "extra.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestDeleteExtraneous_NestedEmptyDirs(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))

	// Destination has a nested directory structure not in source.
	require.NoError(t, os.MkdirAll(filepath.Join(dst, "old", "deep", "nested"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "old", "deep", "nested", "file.txt"), []byte("x"), 0644))

	deleted, err := DeleteExtraneous(context.Background(), DeleteConfig{
		SrcRoot: src,
		DstRoot: dst,
	})

	require.NoError(t, err)
	assert.Greater(t, deleted, 0)

	// The entire old/ tree should be gone.
	_, err = os.Stat(filepath.Join(dst, "old"))
	assert.True(t, os.IsNotExist(err))
}

func TestDeleteExtraneous_EmitsEvents(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "extra.txt"), []byte("x"), 0644))

	events := make(chan event.Event, 10)

	deleted, err := DeleteExtraneous(context.Background(), DeleteConfig{
		SrcRoot: src,
		DstRoot: dst,
		Events:  events,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	// Should have received a DeleteFile event.
	select {
	case ev := <-events:
		assert.Equal(t, event.DeleteFile, ev.Type)
		assert.Equal(t, "extra.txt", ev.Path)
	default:
		t.Fatal("expected a DeleteFile event")
	}
}
