//go:build !linux

package platform

// IOURingCopier is a no-op stub on non-Linux platforms.
type IOURingCopier struct{}

// NewIOURingCopier always returns (nil, nil) on non-Linux platforms.
func NewIOURingCopier(_ uint) (*IOURingCopier, error) {
	return nil, nil
}

func (c *IOURingCopier) Close() error { return nil }

func (c *IOURingCopier) CopyFile(_ CopyFileParams) (CopyResult, error) {
	return CopyResult{}, nil
}

func (c *IOURingCopier) CopyBatch(paramsList []CopyFileParams) ([]CopyResult, []error) {
	results := make([]CopyResult, len(paramsList))
	errs := make([]error, len(paramsList))
	return results, errs
}

// KernelSupportsIOURing always returns false on non-Linux platforms.
func KernelSupportsIOURing() bool {
	return false
}
