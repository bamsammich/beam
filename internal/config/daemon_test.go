package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/config"
)

// setTestDiscoveryPath overrides the discovery path for a test and restores it
// after the test completes.
func setTestDiscoveryPath(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "beam", "daemon.toml")
	config.SetDaemonDiscoveryPathOverride(path)
	t.Cleanup(func() { config.SetDaemonDiscoveryPathOverride("") })
}

func TestWriteDaemonDiscovery(t *testing.T) {
	dir := t.TempDir()
	setTestDiscoveryPath(t, dir)

	err := config.WriteDaemonDiscovery(config.DaemonDiscovery{
		Fingerprint: "SHA256:testfingerprint123",
		Port:        9876,
	})
	require.NoError(t, err)

	// File should exist at the overridden path.
	path := filepath.Join(dir, "beam", "daemon.toml")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(data), `fingerprint = "SHA256:testfingerprint123"`)
	assert.Contains(t, string(data), "port = 9876")

	// File permissions should be world-readable.
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())
}

func TestReadDaemonDiscovery(t *testing.T) {
	dir := t.TempDir()
	setTestDiscoveryPath(t, dir)

	configDir := filepath.Join(dir, "beam")
	require.NoError(t, os.MkdirAll(configDir, 0o755))

	content := "fingerprint = \"SHA256:abc\"\nport = 1234\n"
	require.NoError(t, os.WriteFile(
		filepath.Join(configDir, "daemon.toml"), []byte(content), 0o644,
	))

	d, err := config.ReadDaemonDiscovery()
	require.NoError(t, err)
	assert.Equal(t, "SHA256:abc", d.Fingerprint)
	assert.Equal(t, 1234, d.Port)
}

func TestReadDaemonDiscovery_Missing(t *testing.T) {
	dir := t.TempDir()
	setTestDiscoveryPath(t, dir)

	_, err := config.ReadDaemonDiscovery()
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestRemoveDaemonDiscovery(t *testing.T) {
	dir := t.TempDir()
	setTestDiscoveryPath(t, dir)

	// Write then remove.
	require.NoError(t, config.WriteDaemonDiscovery(config.DaemonDiscovery{
		Fingerprint: "x",
		Port:        1,
	}))
	config.RemoveDaemonDiscovery()

	path := filepath.Join(dir, "beam", "daemon.toml")
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestDaemonDiscoveryPath(t *testing.T) {
	assert.Equal(t, "/etc/beam/daemon.toml", config.DaemonDiscoveryPath())
}
