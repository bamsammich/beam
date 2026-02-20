//go:build darwin

package platform

import (
	"golang.org/x/sys/unix"
)

// CopyFile tries clonefile first (for whole-file CoW copies), then falls back
// to read/write on macOS.
func CopyFile(params CopyFileParams) (CopyResult, error) {
	// clonefile only works for whole-file copies.
	if params.SrcOffset == 0 && params.Length == 0 {
		err := unix.Clonefile(params.SrcPath, params.DstFd.Name(), 0)
		if err == nil {
			return CopyResult{BytesWritten: params.SrcSize, Method: Clonefile}, nil
		}
		if !isFallbackCloneErr(err) {
			return CopyResult{}, err
		}
	}

	preallocate(params.DstFd, copyLength(params))
	return copyReadWrite(params)
}

func isFallbackCloneErr(err error) bool {
	switch err {
	case unix.ENOTSUP, unix.EXDEV, unix.EEXIST:
		return true
	}
	return false
}
