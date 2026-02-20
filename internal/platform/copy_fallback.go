//go:build !linux && !darwin

package platform

// CopyFile falls back to read/write on unsupported platforms.
func CopyFile(params CopyFileParams) (CopyResult, error) {
	preallocate(params.DstFd, copyLength(params))
	return copyReadWrite(params)
}
