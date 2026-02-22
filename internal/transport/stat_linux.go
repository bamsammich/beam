//go:build linux

package transport

import (
	"syscall"
	"time"
)

// fillStatFields extracts platform-specific fields from syscall.Stat_t into a FileEntry.
func fillStatFields(stat *syscall.Stat_t, entry *FileEntry) {
	entry.Dev = stat.Dev
	entry.Ino = stat.Ino
	entry.AccTime = time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
}
