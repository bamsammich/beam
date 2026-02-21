package filter

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	filterFile := filepath.Join(dir, "filter.rules")

	content := `# This is a comment
+ *.go
- *.log

- build/
noprefix.txt
`
	require.NoError(t, os.WriteFile(filterFile, []byte(content), 0644))

	c := NewChain()
	require.NoError(t, c.LoadFile(filterFile))

	// Rules should be in order: include *.go, exclude *.log, exclude build/, exclude noprefix.txt.
	assert.Len(t, c.rules, 4)
	assert.True(t, c.rules[0].Include)
	assert.False(t, c.rules[1].Include)
	assert.False(t, c.rules[2].Include)
	assert.False(t, c.rules[3].Include)

	// Test matching.
	assert.True(t, c.Match("main.go", false, 100))
	assert.False(t, c.Match("app.log", false, 100))
	assert.False(t, c.Match("build", true, 0))
	assert.False(t, c.Match("noprefix.txt", false, 100))
}

func TestLoadFileEmpty(t *testing.T) {
	dir := t.TempDir()
	filterFile := filepath.Join(dir, "empty.rules")
	require.NoError(t, os.WriteFile(filterFile, []byte("# only comments\n\n"), 0644))

	c := NewChain()
	require.NoError(t, c.LoadFile(filterFile))
	assert.Empty(t, c.rules)
}

func TestLoadFileNotExists(t *testing.T) {
	c := NewChain()
	err := c.LoadFile("/nonexistent/path")
	assert.Error(t, err)
}

func TestLoadFileComments(t *testing.T) {
	dir := t.TempDir()
	filterFile := filepath.Join(dir, "comments.rules")

	content := `# comment 1
# comment 2
- *.tmp
# another comment
+ keep.tmp
`
	require.NoError(t, os.WriteFile(filterFile, []byte(content), 0644))

	c := NewChain()
	require.NoError(t, c.LoadFile(filterFile))
	assert.Len(t, c.rules, 2)
}
