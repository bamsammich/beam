package transport

import (
	"io"
	"os"
	"time"
)

// FileEntry describes a single filesystem entry with full metadata.
type FileEntry struct {
	ModTime    time.Time
	AccTime    time.Time
	LinkTarget string
	RelPath    string
	Size       int64
	Ino        uint64
	Dev        uint64
	GID        uint32
	UID        uint32
	Nlink      uint32
	Mode       os.FileMode
	IsSymlink  bool
	IsDir      bool
}

// Capabilities describes what a transport endpoint supports.
type Capabilities struct {
	SparseDetect  bool
	Hardlinks     bool
	Xattrs        bool
	AtomicRename  bool
	FastCopy      bool // local kernel-offload copy (copy_file_range, clonefile)
	NativeHash    bool // can hash without transferring bytes to caller
	DeltaTransfer bool // server supports ComputeSignature, MatchBlocks, ApplyDelta RPCs
	BatchWrite    bool // server supports WriteFileBatch RPC for small-file batching
}

// MetadataOpts controls which metadata to set.
type MetadataOpts struct {
	Mode  bool
	Times bool
	Owner bool
	Xattr bool
}

// WriteFile is a writable temp file on the endpoint.
type WriteFile interface {
	io.WriteCloser
	Name() string
}

// ReadEndpoint is the source side of a transfer.
type ReadEndpoint interface {
	// Walk recursively walks the tree rooted at the endpoint, calling fn for
	// each entry. relPath is relative to the endpoint root.
	Walk(fn func(entry FileEntry) error) error

	// Stat returns metadata for a single relative path.
	Stat(relPath string) (FileEntry, error)

	// ReadDir lists immediate children of a relative directory path.
	ReadDir(relPath string) ([]FileEntry, error)

	// OpenRead opens a file for reading by relative path.
	OpenRead(relPath string) (io.ReadCloser, error)

	// Hash computes the BLAKE3 hash of a file by relative path.
	Hash(relPath string) (string, error)

	// Root returns the absolute root path of this endpoint.
	Root() string

	// Caps returns the capabilities of this endpoint.
	Caps() Capabilities

	// Close releases resources held by this endpoint.
	Close() error
}

// WriteEndpoint is the destination side of a transfer.
type WriteEndpoint interface {
	// MkdirAll creates a directory and all parents.
	MkdirAll(relPath string, perm os.FileMode) error

	// CreateTemp creates a temporary file in the same directory as relPath.
	// The caller must close the returned WriteFile.
	CreateTemp(relPath string, perm os.FileMode) (WriteFile, error)

	// Rename atomically moves oldRel to newRel (both relative to root).
	Rename(oldRel, newRel string) error

	// Remove deletes a single file.
	Remove(relPath string) error

	// RemoveAll recursively deletes a directory.
	RemoveAll(relPath string) error

	// Symlink creates a symbolic link at newRel pointing to target.
	Symlink(target, newRel string) error

	// Link creates a hard link at newRel pointing to oldRel.
	Link(oldRel, newRel string) error

	// SetMetadata sets file metadata according to opts.
	SetMetadata(relPath string, entry FileEntry, opts MetadataOpts) error

	// Walk recursively walks the tree rooted at the endpoint.
	Walk(fn func(entry FileEntry) error) error

	// Stat returns metadata for a single relative path.
	Stat(relPath string) (FileEntry, error)

	// OpenRead opens an existing file for reading (used by delta transfer
	// to read the basis file on the destination).
	OpenRead(relPath string) (io.ReadCloser, error)

	// Hash computes the BLAKE3 hash of a file by relative path.
	Hash(relPath string) (string, error)

	// Root returns the absolute root path of this endpoint.
	Root() string

	// Caps returns the capabilities of this endpoint.
	Caps() Capabilities

	// Close releases resources held by this endpoint.
	Close() error
}
