//go:build darwin

package engine

import (
	"fmt"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// atimeFromStat returns the access time from a syscall.Stat_t.
func atimeFromStat(stat *syscall.Stat_t) time.Time {
	return time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec)
}

// devFromStat returns the device number from a syscall.Stat_t.
func devFromStat(stat *syscall.Stat_t) uint64 {
	return uint64(stat.Dev) //nolint:gosec // G115: dev_t is int32 on darwin, always non-negative
}

// setFileTimes sets mtime (and optionally atime) on a file by path.
// Darwin lacks UTIME_OMIT and AT_EMPTY_PATH, so we always use path-based utimensat.
func setFileTimes(
	_ int,
	fdPath string,
	accTime time.Time,
	modTime time.Time,
	preserveAtime bool,
) error {
	atime := unix.NsecToTimespec(modTime.UnixNano())
	if preserveAtime {
		atime = unix.NsecToTimespec(accTime.UnixNano())
	}
	times := []unix.Timespec{
		atime,
		unix.NsecToTimespec(modTime.UnixNano()),
	}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, fdPath, times, 0); err != nil {
		return fmt.Errorf("utimensat: %w", err)
	}
	return nil
}
