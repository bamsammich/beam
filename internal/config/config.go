package config

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// Config represents the optional beam configuration file.
type Config struct {
	Defaults DefaultsConfig `toml:"defaults"`
	Theme    ThemeConfig    `toml:"theme"`
}

// DefaultsConfig holds persistent flag defaults.
type DefaultsConfig struct {
	Verify  *bool   `toml:"verify"`
	Workers *int    `toml:"workers"`
	TUI     *bool   `toml:"tui"`
	Archive *bool   `toml:"archive"`
	BWLimit *string `toml:"bwlimit"`
}

// ThemeConfig holds optional color overrides.
type ThemeConfig struct {
	Green  *string `toml:"green"`
	Blue   *string `toml:"blue"`
	Yellow *string `toml:"yellow"`
	Red    *string `toml:"red"`
	Teal   *string `toml:"teal"`
	Mauve  *string `toml:"mauve"`
	Muted  *string `toml:"muted"`
	Dim    *string `toml:"dim"`
	Bright *string `toml:"bright"`
}

// Path returns the resolved path to the config file.
func Path() string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "beam", "config.toml")
}

// Load reads the config file from the XDG path. Returns a zero Config
// (no error) if the file does not exist. Config is always optional.
func Load() (Config, error) {
	path := Path()
	if path == "" {
		return Config{}, nil
	}

	var cfg Config
	_, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{}, nil
		}
		return Config{}, err
	}
	return cfg, nil
}
