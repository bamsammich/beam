//go:build linux

package platform

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocate attempts to pre-allocate disk space. Errors are ignored as
// fallocate is not supported on all filesystems.
//
//nolint:gosec // G115: fd values are small non-negative integers
func preallocate(fd *os.File, size int64) {
	//nolint:errcheck // fallocate is advisory; not supported on all filesystems
	unix.Fallocate(int(fd.Fd()), 0, 0, size)
}
