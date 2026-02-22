package transport

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Location represents a parsed source or destination argument.
type Location struct {
	Host string // empty for local
	User string // empty = current user
	Port int    // 0 = default (22)
	Path string // absolute or relative path
}

// IsRemote returns true if the location refers to a remote host.
func (l Location) IsRemote() bool {
	return l.Host != ""
}

// String returns a human-readable representation.
func (l Location) String() string {
	if !l.IsRemote() {
		return l.Path
	}
	if l.User != "" {
		return fmt.Sprintf("%s@%s:%s", l.User, l.Host, l.Path)
	}
	return fmt.Sprintf("%s:%s", l.Host, l.Path)
}

// ParseLocation parses a CLI argument into a Location.
//
// Supported formats:
//   - /absolute/path        → local
//   - relative/path         → local
//   - host:path             → remote (current user)
//   - user@host:path        → remote
//   - user@host:/abs/path   → remote
//
// Ambiguity rule: a bare "word" with no colon is always local. A path
// containing ":" is only treated as remote if the part before the colon
// contains no path separators (so "/foo:bar" and "./host:path" are local).
func ParseLocation(arg string) Location {
	// Absolute paths and paths starting with . are always local.
	if filepath.IsAbs(arg) || strings.HasPrefix(arg, "./") || strings.HasPrefix(arg, "../") {
		return Location{Path: arg}
	}

	// Look for user@host:path or host:path pattern.
	colonIdx := strings.IndexByte(arg, ':')
	if colonIdx < 0 {
		// No colon — local path.
		return Location{Path: arg}
	}

	hostPart := arg[:colonIdx]
	pathPart := arg[colonIdx+1:]

	// If the host part contains a path separator, it's a local path with
	// a colon in it (e.g., "dir/file:with:colons").
	if strings.ContainsRune(hostPart, filepath.Separator) || strings.ContainsRune(hostPart, '/') {
		return Location{Path: arg}
	}

	// Empty host part means bare ":path" which is not valid remote syntax.
	if hostPart == "" {
		return Location{Path: arg}
	}

	// Parse user@host.
	var user, host string
	if atIdx := strings.LastIndexByte(hostPart, '@'); atIdx >= 0 {
		user = hostPart[:atIdx]
		host = hostPart[atIdx+1:]
	} else {
		host = hostPart
	}

	if host == "" {
		return Location{Path: arg}
	}

	return Location{
		Host: host,
		User: user,
		Path: pathPart,
	}
}
