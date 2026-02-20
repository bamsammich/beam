//go:build linux

package platform

import (
	"os"

	"golang.org/x/sys/unix"
)

// CopyFile tries the most efficient copy method available on Linux,
// falling through on unsupported/cross-device errors.
func CopyFile(params CopyFileParams) (CopyResult, error) {
	size := copyLength(params)
	preallocate(params.DstFd, size)

	// Try copy_file_range first.
	result, err := copyFileRange(params)
	if err == nil {
		return result, nil
	}
	if !isFallbackErr(err) {
		return result, err
	}

	// Try sendfile.
	result, err = copySendfile(params)
	if err == nil {
		return result, nil
	}
	if !isFallbackErr(err) {
		return result, err
	}

	// Fall back to read/write.
	return copyReadWrite(params)
}

func copyFileRange(params CopyFileParams) (CopyResult, error) {
	srcFd, err := os.Open(params.SrcPath)
	if err != nil {
		return CopyResult{}, err
	}
	defer srcFd.Close()

	remaining := copyLength(params)
	roff := params.SrcOffset
	woff := params.SrcOffset

	var totalWritten int64
	for remaining > 0 {
		n, err := unix.CopyFileRange(int(srcFd.Fd()), &roff, int(params.DstFd.Fd()), &woff, int(remaining), 0)
		if err != nil {
			if totalWritten == 0 {
				return CopyResult{}, err
			}
			return CopyResult{BytesWritten: totalWritten, Method: CopyFileRange}, err
		}
		if n == 0 {
			break
		}
		remaining -= int64(n)
		totalWritten += int64(n)
	}

	return CopyResult{BytesWritten: totalWritten, Method: CopyFileRange}, nil
}

func copySendfile(params CopyFileParams) (CopyResult, error) {
	srcFd, err := os.Open(params.SrcPath)
	if err != nil {
		return CopyResult{}, err
	}
	defer srcFd.Close()

	remaining := copyLength(params)
	offset := params.SrcOffset

	// Seek destination to the correct offset.
	if offset > 0 {
		if _, err := params.DstFd.Seek(offset, 0); err != nil {
			return CopyResult{}, err
		}
	}

	var totalWritten int64
	for remaining > 0 {
		n, err := unix.Sendfile(int(params.DstFd.Fd()), int(srcFd.Fd()), &offset, int(remaining))
		if err != nil {
			if totalWritten == 0 {
				return CopyResult{}, err
			}
			return CopyResult{BytesWritten: totalWritten, Method: Sendfile}, err
		}
		if n == 0 {
			break
		}
		remaining -= int64(n)
		totalWritten += int64(n)
	}

	return CopyResult{BytesWritten: totalWritten, Method: Sendfile}, nil
}
