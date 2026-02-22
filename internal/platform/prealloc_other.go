//go:build !linux

package platform

import "os"

// preallocate is a no-op on non-Linux platforms (fallocate is Linux-only).
func preallocate(_ *os.File, _ int64) {}
