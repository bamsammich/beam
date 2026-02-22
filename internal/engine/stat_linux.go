//go:build linux

package engine

import (
	"fmt"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// atimeFromStat returns the access time from a syscall.Stat_t.
func atimeFromStat(stat *syscall.Stat_t) time.Time {
	return time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
}

// devFromStat returns the device number from a syscall.Stat_t.
func devFromStat(stat *syscall.Stat_t) uint64 {
	return stat.Dev
}

// setFileTimes sets mtime (and optionally atime) on an open file descriptor.
func setFileTimes(
	rawFd int,
	fdPath string,
	accTime time.Time,
	modTime time.Time,
	preserveAtime bool,
) error {
	atime := unix.Timespec{Nsec: unix.UTIME_OMIT}
	if preserveAtime {
		atime = unix.NsecToTimespec(accTime.UnixNano())
	}
	times := []unix.Timespec{
		atime,
		unix.NsecToTimespec(modTime.UnixNano()),
	}
	if err := unix.UtimesNanoAt(rawFd, "", times, unix.AT_EMPTY_PATH); err != nil {
		// Fallback: some systems don't support AT_EMPTY_PATH.
		if err2 := unix.UtimesNanoAt(unix.AT_FDCWD, fdPath, times, 0); err2 != nil {
			return fmt.Errorf("utimensat: %w", err)
		}
	}
	return nil
}
