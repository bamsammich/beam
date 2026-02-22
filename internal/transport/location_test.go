package transport_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bamsammich/beam/internal/transport"
)

func TestParseLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantHost string
		wantUser string
		wantPath string
	}{
		{
			name:     "absolute path",
			input:    "/home/user/data",
			wantPath: "/home/user/data",
		},
		{
			name:     "relative path",
			input:    "data/files",
			wantPath: "data/files",
		},
		{
			name:     "dot-relative path",
			input:    "./data/files",
			wantPath: "./data/files",
		},
		{
			name:     "parent-relative path",
			input:    "../data/files",
			wantPath: "../data/files",
		},
		{
			name:     "user@host:path",
			input:    "user@nas:/backup/data",
			wantHost: "nas",
			wantUser: "user",
			wantPath: "/backup/data",
		},
		{
			name:     "host:path",
			input:    "nas:/backup/data",
			wantHost: "nas",
			wantPath: "/backup/data",
		},
		{
			name:     "user@host:relative",
			input:    "user@nas:backup/data",
			wantHost: "nas",
			wantUser: "user",
			wantPath: "backup/data",
		},
		{
			name:     "host:relative",
			input:    "myserver:files",
			wantHost: "myserver",
			wantPath: "files",
		},
		{
			name:     "host with dots",
			input:    "user@nas.local:/data",
			wantHost: "nas.local",
			wantUser: "user",
			wantPath: "/data",
		},
		{
			name:     "absolute path with colon in filename",
			input:    "/path/to/file:with:colons",
			wantPath: "/path/to/file:with:colons",
		},
		{
			name:     "relative path with colon after separator",
			input:    "dir/host:path",
			wantPath: "dir/host:path",
		},
		{
			name:     "dot-relative with colon",
			input:    "./host:path",
			wantPath: "./host:path",
		},
		{
			name:     "bare colon",
			input:    ":path",
			wantPath: ":path",
		},
		{
			name:     "empty host after @",
			input:    "user@:path",
			wantPath: "user@:path",
		},
		{
			name:     "host:empty path",
			input:    "host:",
			wantHost: "host",
			wantPath: "",
		},
		{
			name:     "user@host:empty path",
			input:    "user@host:",
			wantHost: "host",
			wantUser: "user",
			wantPath: "",
		},
		{
			name:     "simple filename no colon",
			input:    "myfile.txt",
			wantPath: "myfile.txt",
		},
		{
			name:     "trailing slash local",
			input:    "/data/photos/",
			wantPath: "/data/photos/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			loc := transport.ParseLocation(tt.input)
			assert.Equal(t, tt.wantHost, loc.Host, "Host")
			assert.Equal(t, tt.wantUser, loc.User, "User")
			assert.Equal(t, tt.wantPath, loc.Path, "Path")

			if tt.wantHost != "" {
				assert.True(t, loc.IsRemote(), "should be remote")
			} else {
				assert.False(t, loc.IsRemote(), "should be local")
			}
		})
	}
}

func TestParseLocationBeamURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantHost  string
		wantPath  string
		wantToken string
		wantPort  int
		wantBeam  bool
	}{
		{
			name:     "beam with host and path",
			input:    "beam://nas:7223/backup",
			wantHost: "nas",
			wantPort: 7223,
			wantPath: "/backup",
			wantBeam: true,
		},
		{
			name:      "beam with token",
			input:     "beam://mytoken@nas:7223/data",
			wantHost:  "nas",
			wantPort:  7223,
			wantPath:  "/data",
			wantToken: "mytoken",
			wantBeam:  true,
		},
		{
			name:     "beam default port",
			input:    "beam://host/path",
			wantHost: "host",
			wantPort: 0, // 0 = use default
			wantPath: "/path",
			wantBeam: true,
		},
		{
			name:      "beam with token default port",
			input:     "beam://secret@mynas/backup/photos",
			wantHost:  "mynas",
			wantPort:  0,
			wantPath:  "/backup/photos",
			wantToken: "secret",
			wantBeam:  true,
		},
		{
			name:     "beam root path",
			input:    "beam://nas:7223/",
			wantHost: "nas",
			wantPort: 7223,
			wantPath: "/",
			wantBeam: true,
		},
		{
			name:     "beam no path",
			input:    "beam://nas:7223",
			wantHost: "nas",
			wantPort: 7223,
			wantPath: "/",
			wantBeam: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			loc := transport.ParseLocation(tt.input)
			assert.Equal(t, tt.wantHost, loc.Host, "Host")
			assert.Equal(t, tt.wantPort, loc.Port, "Port")
			assert.Equal(t, tt.wantPath, loc.Path, "Path")
			assert.Equal(t, tt.wantToken, loc.Token, "Token")
			assert.Equal(t, tt.wantBeam, loc.IsBeam(), "IsBeam")
			assert.True(t, loc.IsRemote(), "should be remote")
		})
	}
}

func TestLocation_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		loc  transport.Location
	}{
		{
			name: "local",
			loc:  transport.Location{Path: "/data/files"},
			want: "/data/files",
		},
		{
			name: "remote with user",
			loc:  transport.Location{Host: "nas", User: "admin", Path: "/backup"},
			want: "admin@nas:/backup",
		},
		{
			name: "remote without user",
			loc:  transport.Location{Host: "nas", Path: "/backup"},
			want: "nas:/backup",
		},
		{
			name: "beam with token",
			loc: transport.Location{
				Scheme: "beam",
				Host:   "nas",
				Port:   7223,
				Path:   "/backup",
				Token:  "mytoken",
			},
			want: "beam://mytoken@nas:7223/backup",
		},
		{
			name: "beam without token",
			loc:  transport.Location{Scheme: "beam", Host: "nas", Port: 7223, Path: "/data"},
			want: "beam://nas:7223/data",
		},
		{
			name: "beam default port",
			loc:  transport.Location{Scheme: "beam", Host: "nas", Path: "/data"},
			want: "beam://nas:7223/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.loc.String())
		})
	}
}
