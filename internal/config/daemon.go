package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// SystemDaemonDiscoveryPath is the well-known system-wide path for the daemon
// discovery file. This path is readable by all users (0644) and writable by
// root/daemon process, making it accessible to SSH users connecting to discover
// a running beam daemon.
const SystemDaemonDiscoveryPath = "/etc/beam/daemon.toml"

// daemonDiscoveryPathOverride allows tests to redirect the discovery file path.
// When empty, DaemonDiscoveryPath() returns SystemDaemonDiscoveryPath.
var daemonDiscoveryPathOverride string //nolint:gochecknoglobals // test hook

// SetDaemonDiscoveryPathOverride sets a test override for the discovery path.
// Pass "" to restore the default. This is intended for tests only.
func SetDaemonDiscoveryPathOverride(path string) {
	daemonDiscoveryPathOverride = path
}

// DaemonDiscovery holds daemon connection info written to /etc/beam/daemon.toml.
// Remote clients read this file over SFTP to discover and authenticate with a
// running beam daemon.
type DaemonDiscovery struct {
	Token string `toml:"token"`
	Port  int    `toml:"port"`
}

// DaemonDiscoveryPath returns the path to the daemon discovery file.
func DaemonDiscoveryPath() string {
	if daemonDiscoveryPathOverride != "" {
		return daemonDiscoveryPathOverride
	}
	return SystemDaemonDiscoveryPath
}

// WriteDaemonDiscovery writes the daemon discovery file with world-readable
// permissions (0644). Creates the parent directory if needed.
func WriteDaemonDiscovery(d DaemonDiscovery) error {
	path := DaemonDiscoveryPath()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(d); err != nil {
		return fmt.Errorf("encode daemon discovery: %w", err)
	}

	//nolint:gosec // G306: world-readable is intentional for SSH user discovery
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// ReadDaemonDiscovery reads the daemon discovery file. Returns os.ErrNotExist
// if the file does not exist.
func ReadDaemonDiscovery() (DaemonDiscovery, error) {
	path := DaemonDiscoveryPath()

	var d DaemonDiscovery
	_, err := toml.DecodeFile(path, &d)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return DaemonDiscovery{}, os.ErrNotExist
		}
		return DaemonDiscovery{}, err
	}
	return d, nil
}

// RemoveDaemonDiscovery removes the daemon discovery file (best-effort).
func RemoveDaemonDiscovery() {
	os.Remove(DaemonDiscoveryPath()) //nolint:errcheck // best-effort cleanup on shutdown
}
