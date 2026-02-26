//go:build darwin

package transport

import (
	"syscall"
	"time"
)

// fillStatFields extracts platform-specific fields from syscall.Stat_t into a FileEntry.
func fillStatFields(stat *syscall.Stat_t, entry *FileEntry) {
	entry.Dev = uint64(
		stat.Dev,
	) //nolint:gosec // G115: dev_t is int32 on darwin, always non-negative
	entry.Ino = stat.Ino
	entry.AccTime = time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec)
}
