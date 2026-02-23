package beam

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputePathPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		root       string
		daemonRoot string
		want       string
	}{
		{
			name:       "root equals daemonRoot",
			root:       "/srv/data",
			daemonRoot: "/srv/data",
			want:       "",
		},
		{
			name:       "root inside daemonRoot",
			root:       "/srv/data/subdir",
			daemonRoot: "/srv/data",
			want:       "subdir",
		},
		{
			name:       "root deeply inside daemonRoot",
			root:       "/srv/data/a/b/c",
			daemonRoot: "/srv/data",
			want:       "a/b/c",
		},
		{
			name:       "daemonRoot is filesystem root",
			root:       "/home/thudd/dst",
			daemonRoot: "/",
			want:       "home/thudd/dst",
		},
		{
			name:       "root outside daemonRoot — must not escape",
			root:       "/other/path",
			daemonRoot: "/srv/data",
			want:       "",
		},
		{
			name:       "root is / with specific daemonRoot — must not escape",
			root:       "/",
			daemonRoot: "/tmp/src",
			want:       "",
		},
		{
			name:       "root is parent of daemonRoot — must not escape",
			root:       "/srv",
			daemonRoot: "/srv/data",
			want:       "",
		},
		{
			name:       "root shares prefix but diverges — must not escape",
			root:       "/srv/data2",
			daemonRoot: "/srv/data",
			want:       "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := computePathPrefix(tc.root, tc.daemonRoot)
			assert.Equal(t, tc.want, got)
		})
	}
}
