package transport

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

// DefaultBeamPort is the default port for the beam protocol daemon.
const DefaultBeamPort = 9876

// Location represents a parsed source or destination argument.
type Location struct {
	Scheme string
	Host   string
	User   string
	Path   string
	Token  string
	Port   int
}

// IsRemote returns true if the location refers to a remote host.
func (l Location) IsRemote() bool {
	return l.Host != ""
}

// IsBeam returns true if the location uses the beam:// protocol.
func (l Location) IsBeam() bool {
	return l.Scheme == "beam"
}

// String returns a human-readable representation.
func (l Location) String() string {
	if l.IsBeam() {
		port := l.Port
		if port == 0 {
			port = DefaultBeamPort
		}
		if l.Token != "" {
			return fmt.Sprintf("beam://%s@%s:%d%s", l.Token, l.Host, port, l.Path)
		}
		return fmt.Sprintf("beam://%s:%d%s", l.Host, port, l.Path)
	}
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
//   - /absolute/path                  → local
//   - relative/path                   → local
//   - host:path                       → SSH remote (current user)
//   - user@host:path                  → SSH remote
//   - user@host:/abs/path             → SSH remote
//   - beam://host/path                → beam protocol (default port 9876)
//   - beam://host:port/path           → beam protocol
//   - beam://token@host:port/path     → beam protocol with auth token
//
// Ambiguity rule: a bare "word" with no colon is always local. A path
// containing ":" is only treated as remote if the part before the colon
// contains no path separators (so "/foo:bar" and "./host:path" are local).
//
//nolint:revive // cognitive-complexity: location parsing handles multiple format variants
func ParseLocation(arg string) Location {
	// Check for beam:// URL scheme first.
	if strings.HasPrefix(arg, "beam://") {
		return parseBeamURL(arg)
	}

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

// parseBeamURL parses a beam://[token@]host[:port]/path URL.
func parseBeamURL(raw string) Location {
	u, err := url.Parse(raw)
	if err != nil {
		return Location{Path: raw}
	}

	host := u.Hostname()
	if host == "" {
		return Location{Path: raw}
	}

	port := 0
	if p := u.Port(); p != "" {
		port, err = strconv.Atoi(p)
		if err != nil {
			return Location{Path: raw}
		}
	}

	// Path from URL starts with /; keep it as-is for beam protocol
	// (the daemon interprets paths relative to its root).
	path := u.Path
	if path == "" {
		path = "/"
	}

	var token string
	if u.User != nil {
		token = u.User.Username()
	}

	return Location{
		Scheme: "beam",
		Host:   host,
		Port:   port,
		Path:   path,
		Token:  token,
	}
}
