package transport_test

import (
	"testing"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/stretchr/testify/assert"
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

func TestLocation_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		loc  transport.Location
		want string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.loc.String())
		})
	}
}
