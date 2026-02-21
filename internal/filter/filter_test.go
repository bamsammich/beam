package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyChainIncludesAll(t *testing.T) {
	c := NewChain()
	assert.True(t, c.Match("any/file.txt", false, 1024))
	assert.True(t, c.Match("any/dir", true, 0))
	assert.True(t, c.Empty())
}

func TestExcludePattern(t *testing.T) {
	c := NewChain()
	require.NoError(t, c.AddExclude("*.log"))

	assert.False(t, c.Match("app.log", false, 100))
	assert.False(t, c.Match("sub/debug.log", false, 100))
	assert.True(t, c.Match("app.txt", false, 100))
}

func TestIncludeOverridesExclude(t *testing.T) {
	c := NewChain()
	require.NoError(t, c.AddInclude("important.log"))
	require.NoError(t, c.AddExclude("*.log"))

	// include rule matches first for important.log.
	assert.True(t, c.Match("important.log", false, 100))
	// other .log files are excluded.
	assert.False(t, c.Match("debug.log", false, 100))
}

func TestExcludeIncludeOrder(t *testing.T) {
	// rsync: --exclude '*.log' --include 'important.log'
	// exclude comes first, so important.log is also excluded.
	c := NewChain()
	require.NoError(t, c.AddExclude("*.log"))
	require.NoError(t, c.AddInclude("important.log"))

	assert.False(t, c.Match("important.log", false, 100))
	assert.False(t, c.Match("debug.log", false, 100))
}

func TestDirOnlyPattern(t *testing.T) {
	c := NewChain()
	require.NoError(t, c.AddExclude("build/"))

	assert.False(t, c.Match("build", true, 0))
	assert.True(t, c.Match("build", false, 100)) // file named "build" is not excluded
}

func TestAnchoredPattern(t *testing.T) {
	c := NewChain()
	require.NoError(t, c.AddExclude("/root.txt"))

	assert.False(t, c.Match("root.txt", false, 100))
	assert.True(t, c.Match("sub/root.txt", false, 100))
}

func TestDoubleStarGo(t *testing.T) {
	c := NewChain()
	require.NoError(t, c.AddInclude("**/*.go"))
	require.NoError(t, c.AddExclude("*"))

	assert.True(t, c.Match("main.go", false, 100))
	assert.True(t, c.Match("internal/engine/engine.go", false, 100))
	assert.False(t, c.Match("readme.md", false, 100))
}

func TestSizeFilters(t *testing.T) {
	c := NewChain()
	c.SetMinSize(100)
	c.SetMaxSize(10000)

	assert.False(t, c.Match("tiny.txt", false, 50))
	assert.True(t, c.Match("medium.txt", false, 500))
	assert.False(t, c.Match("huge.bin", false, 50000))

	// Directories ignore size filters.
	assert.True(t, c.Match("somedir", true, 0))
}

func TestMinSizeOnly(t *testing.T) {
	c := NewChain()
	c.SetMinSize(1024 * 1024) // 1M

	assert.False(t, c.Match("small.txt", false, 512))
	assert.True(t, c.Match("big.bin", false, 2*1024*1024))
}

func TestMaxSizeOnly(t *testing.T) {
	c := NewChain()
	c.SetMaxSize(1024 * 1024) // 1M

	assert.True(t, c.Match("small.txt", false, 512))
	assert.False(t, c.Match("big.bin", false, 2*1024*1024))
}
