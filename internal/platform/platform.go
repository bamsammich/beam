package platform

import "os"

// CopyMethod identifies which syscall/strategy was used for a copy.
type CopyMethod int

const (
	ReadWrite     CopyMethod = iota
	CopyFileRange            // Linux copy_file_range(2)
	Sendfile                 // Linux sendfile(2)
	IOURing                  // Linux io_uring
	Clonefile                // macOS clonefile(2)
)

func (m CopyMethod) String() string {
	switch m {
	case ReadWrite:
		return "read_write"
	case CopyFileRange:
		return "copy_file_range"
	case Sendfile:
		return "sendfile"
	case IOURing:
		return "io_uring"
	case Clonefile:
		return "clonefile"
	default:
		return "unknown"
	}
}

// CopyResult reports the outcome of a copy operation.
type CopyResult struct {
	BytesWritten int64
	Method       CopyMethod
}

// CopyFileParams describes what to copy.
type CopyFileParams struct {
	DstFd     *os.File
	SrcPath   string
	SrcOffset int64
	SrcSize   int64
	Length    int64
}
