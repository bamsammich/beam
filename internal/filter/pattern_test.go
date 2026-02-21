package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPatternStar(t *testing.T) {
	p, err := compilePattern("*.log")
	require.NoError(t, err)

	// Matches basename.
	assert.True(t, p.match("app.log", false))
	assert.True(t, p.match("dir/app.log", false))

	// Does not match partial.
	assert.False(t, p.match("app.log.bak", false))
	assert.False(t, p.match("app.txt", false))
}

func TestPatternDoubleStar(t *testing.T) {
	p, err := compilePattern("**/*.go")
	require.NoError(t, err)

	assert.True(t, p.match("main.go", false))
	assert.True(t, p.match("cmd/beam/main.go", false))
	assert.True(t, p.match("internal/engine/engine.go", false))
	assert.False(t, p.match("main.txt", false))
}

func TestPatternAnchored(t *testing.T) {
	p, err := compilePattern("/root.txt")
	require.NoError(t, err)

	assert.True(t, p.match("root.txt", false))
	assert.False(t, p.match("sub/root.txt", false))
}

func TestPatternUnanchoredBasename(t *testing.T) {
	p, err := compilePattern("*.tmp")
	require.NoError(t, err)

	assert.True(t, p.match("file.tmp", false))
	assert.True(t, p.match("a/b/c/file.tmp", false))
}

func TestPatternDirOnly(t *testing.T) {
	p, err := compilePattern("build/")
	require.NoError(t, err)

	assert.True(t, p.match("build", true))
	assert.True(t, p.match("sub/build", true))
	assert.False(t, p.match("build", false)) // not a dir
}

func TestPatternQuestion(t *testing.T) {
	p, err := compilePattern("file?.txt")
	require.NoError(t, err)

	assert.True(t, p.match("file1.txt", false))
	assert.True(t, p.match("fileA.txt", false))
	assert.False(t, p.match("file12.txt", false))
	assert.False(t, p.match("file/.txt", false)) // ? does not match /
}

func TestPatternContainingSlash(t *testing.T) {
	// Pattern containing / but not leading / is anchored per rsync.
	p, err := compilePattern("sub/dir/*.txt")
	require.NoError(t, err)

	assert.True(t, p.match("sub/dir/file.txt", false))
	assert.False(t, p.match("other/sub/dir/file.txt", false))
}
