package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bamsammich/beam/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_MissingFile(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Nil(t, cfg.Defaults.Verify)
	assert.Nil(t, cfg.Defaults.Workers)
	assert.Nil(t, cfg.Theme.Green)
}

func TestLoad_FullConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	configDir := filepath.Join(dir, "beam")
	require.NoError(t, os.MkdirAll(configDir, 0o755))

	content := `
[defaults]
verify = true
workers = 16
tui = true
archive = false
bwlimit = "100MB"

[theme]
green = "#00ff00"
red = "#ff0000"
`
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(content), 0o644))

	cfg, err := config.Load()
	require.NoError(t, err)

	require.NotNil(t, cfg.Defaults.Verify)
	assert.True(t, *cfg.Defaults.Verify)

	require.NotNil(t, cfg.Defaults.Workers)
	assert.Equal(t, 16, *cfg.Defaults.Workers)

	require.NotNil(t, cfg.Defaults.TUI)
	assert.True(t, *cfg.Defaults.TUI)

	require.NotNil(t, cfg.Defaults.Archive)
	assert.False(t, *cfg.Defaults.Archive)

	require.NotNil(t, cfg.Defaults.BWLimit)
	assert.Equal(t, "100MB", *cfg.Defaults.BWLimit)

	require.NotNil(t, cfg.Theme.Green)
	assert.Equal(t, "#00ff00", *cfg.Theme.Green)

	require.NotNil(t, cfg.Theme.Red)
	assert.Equal(t, "#ff0000", *cfg.Theme.Red)

	// Unset fields should remain nil.
	assert.Nil(t, cfg.Theme.Blue)
	assert.Nil(t, cfg.Theme.Bright)
}

func TestLoad_PartialConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	configDir := filepath.Join(dir, "beam")
	require.NoError(t, os.MkdirAll(configDir, 0o755))

	content := `
[theme]
bright = "#ffffff"
`
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(content), 0o644))

	cfg, err := config.Load()
	require.NoError(t, err)

	// Defaults section entirely absent.
	assert.Nil(t, cfg.Defaults.Verify)
	assert.Nil(t, cfg.Defaults.Workers)

	require.NotNil(t, cfg.Theme.Bright)
	assert.Equal(t, "#ffffff", *cfg.Theme.Bright)
}

func TestLoad_InvalidTOML(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	configDir := filepath.Join(dir, "beam")
	require.NoError(t, os.MkdirAll(configDir, 0o755))

	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte("invalid [[["), 0o644))

	_, err := config.Load()
	assert.Error(t, err)
}

func TestConfigPath(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/custom/config")
	assert.Equal(t, "/custom/config/beam/config.toml", config.ConfigPath())
}
