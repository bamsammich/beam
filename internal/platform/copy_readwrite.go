package platform

import (
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const bufferSize = 1 << 20 // 1 MiB

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, bufferSize)
		return &b
	},
}

// copyReadWrite copies data using pread/pwrite with a pooled buffer.
//
//nolint:gosec // G115: fd values are small non-negative integers from os.File.Fd()
func copyReadWrite(params CopyFileParams) (CopyResult, error) {
	srcFd, err := os.Open(params.SrcPath)
	if err != nil {
		return CopyResult{}, err
	}
	defer srcFd.Close()

	bufp, _ := bufPool.Get().(*[]byte) //nolint:revive // unchecked-type-assertion: pool only stores *[]byte
	defer bufPool.Put(bufp)
	buf := *bufp

	offset := params.SrcOffset
	remaining := params.Length
	if remaining == 0 {
		remaining = params.SrcSize - offset
	}

	var totalWritten int64
	srcRawFd := int(srcFd.Fd())
	dstRawFd := int(params.DstFd.Fd())

	for remaining > 0 {
		toRead := int(remaining)
		if toRead > bufferSize {
			toRead = bufferSize
		}

		n, err := unix.Pread(srcRawFd, buf[:toRead], offset)
		if err != nil {
			return CopyResult{BytesWritten: totalWritten, Method: ReadWrite}, err
		}
		if n == 0 {
			break
		}

		written := 0
		for written < n {
			w, err := unix.Pwrite(dstRawFd, buf[written:n], offset+int64(written))
			if err != nil {
				return CopyResult{
					BytesWritten: totalWritten + int64(written),
					Method:       ReadWrite,
				}, err
			}
			written += w
		}

		offset += int64(n)
		remaining -= int64(n)
		totalWritten += int64(n)
	}

	return CopyResult{BytesWritten: totalWritten, Method: ReadWrite}, nil
}

// CopyReadWrite is the exported version for use by other packages during testing.
func CopyReadWrite(params CopyFileParams) (CopyResult, error) {
	return copyReadWrite(params)
}

// copyLength returns the effective byte count to copy.
func copyLength(params CopyFileParams) int64 {
	if params.Length > 0 {
		return params.Length
	}
	return params.SrcSize - params.SrcOffset
}

// isFallbackErr returns true if err should trigger a fallback to the next copy strategy.
func isFallbackErr(err error) bool {
	switch err {
	case unix.ENOSYS, unix.EXDEV, unix.EINVAL, unix.ENOTSUP:
		return true
	}
	// Also handle wrapped errors.
	if e, ok := err.(*os.PathError); ok {
		return isFallbackErr(e.Err)
	}
	return false
}
