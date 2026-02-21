package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	require.NoError(t, os.WriteFile(path, []byte("hello world"), 0644))

	h1, err := HashFile(path)
	require.NoError(t, err)
	assert.NotEmpty(t, h1)

	// Same content should produce the same hash.
	path2 := filepath.Join(dir, "test2.txt")
	require.NoError(t, os.WriteFile(path2, []byte("hello world"), 0644))
	h2, err := HashFile(path2)
	require.NoError(t, err)
	assert.Equal(t, h1, h2)

	// Different content should produce a different hash.
	path3 := filepath.Join(dir, "test3.txt")
	require.NoError(t, os.WriteFile(path3, []byte("different content"), 0644))
	h3, err := HashFile(path3)
	require.NoError(t, err)
	assert.NotEqual(t, h1, h3)
}

func TestHashFileEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.txt")
	require.NoError(t, os.WriteFile(path, nil, 0644))

	h, err := HashFile(path)
	require.NoError(t, err)
	assert.NotEmpty(t, h)
}

func TestHashFileNotExist(t *testing.T) {
	_, err := HashFile("/nonexistent/file")
	assert.Error(t, err)
}
