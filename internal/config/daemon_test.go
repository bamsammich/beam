package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/config"
)

func TestWriteDaemonDiscovery(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	err := config.WriteDaemonDiscovery(config.DaemonDiscovery{
		Token: "test-token-abc123",
		Port:  9876,
	})
	require.NoError(t, err)

	// File should exist at ~/.config/beam/daemon.toml.
	path := filepath.Join(dir, "beam", "daemon.toml")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(data), `token = "test-token-abc123"`)
	assert.Contains(t, string(data), "port = 9876")

	// File permissions should be owner-only.
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestReadDaemonDiscovery(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	configDir := filepath.Join(dir, "beam")
	require.NoError(t, os.MkdirAll(configDir, 0o755))

	content := "token = \"my-token\"\nport = 1234\n"
	require.NoError(t, os.WriteFile(
		filepath.Join(configDir, "daemon.toml"), []byte(content), 0o600,
	))

	d, err := config.ReadDaemonDiscovery()
	require.NoError(t, err)
	assert.Equal(t, "my-token", d.Token)
	assert.Equal(t, 1234, d.Port)
}

func TestReadDaemonDiscovery_Missing(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	_, err := config.ReadDaemonDiscovery()
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestRemoveDaemonDiscovery(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	// Write then remove.
	require.NoError(t, config.WriteDaemonDiscovery(config.DaemonDiscovery{
		Token: "x",
		Port:  1,
	}))
	config.RemoveDaemonDiscovery()

	path := filepath.Join(dir, "beam", "daemon.toml")
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestDaemonDiscoveryPath(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/custom/config")
	assert.Equal(t, "/custom/config/beam/daemon.toml", config.DaemonDiscoveryPath())
}
