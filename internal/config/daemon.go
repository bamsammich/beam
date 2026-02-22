package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// DaemonDiscovery holds daemon connection info written to ~/.config/beam/daemon.toml.
// Remote clients read this file over SFTP to discover and authenticate with a
// running beam daemon.
type DaemonDiscovery struct {
	Token string `toml:"token"`
	Port  int    `toml:"port"`
}

// DaemonDiscoveryPath returns the path to the daemon discovery file.
func DaemonDiscoveryPath() string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "beam", "daemon.toml")
}

// WriteDaemonDiscovery writes the daemon discovery file with owner-only permissions.
// Creates the parent directory if needed.
func WriteDaemonDiscovery(d DaemonDiscovery) error {
	path := DaemonDiscoveryPath()
	if path == "" {
		return errors.New("cannot determine config directory")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(d); err != nil {
		return fmt.Errorf("encode daemon discovery: %w", err)
	}

	return os.WriteFile(path, buf.Bytes(), 0o600)
}

// ReadDaemonDiscovery reads the daemon discovery file. Returns os.ErrNotExist
// if the file does not exist.
func ReadDaemonDiscovery() (DaemonDiscovery, error) {
	path := DaemonDiscoveryPath()
	if path == "" {
		return DaemonDiscovery{}, errors.New("cannot determine config directory")
	}

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
	path := DaemonDiscoveryPath()
	if path != "" {
		os.Remove(path) //nolint:errcheck // best-effort cleanup on shutdown
	}
}
