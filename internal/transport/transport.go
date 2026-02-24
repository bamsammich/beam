package transport

import (
	"io"
	"os"
	"time"
)

// Protocol identifies the transport protocol in use.
type Protocol int

const (
	ProtocolLocal Protocol = iota
	ProtocolSFTP
	ProtocolBeam
)

// Transport manages connection lifecycle and provides Reader/ReadWriter
// views rooted at specific paths.
//
// Optional capabilities (BatchWriter, DeltaSource, DeltaTarget, SubtreeWalker,
// PathResolver) are discovered via type assertion on the returned endpoint.
// The endpoint's Caps() struct declares what it supports; the assertion
// provides the typed method set.
type Transport interface {
	ReaderAt(path string) (Reader, error)
	ReadWriterAt(path string) (ReadWriter, error)
	Protocol() Protocol
	Close() error
}

// BatchWriter is implemented by endpoints that support batched small-file writes.
type BatchWriter interface {
	WriteFileBatch(req BatchWriteRequest) ([]BatchWriteResult, error)
}

// DeltaSource is implemented by read endpoints that support server-side delta operations.
type DeltaSource interface {
	ComputeSignature(relPath string, fileSize int64) (Signature, error)
	MatchBlocks(relPath string, sig Signature) ([]DeltaOp, error)
}

// DeltaTarget is implemented by write endpoints that support server-side delta operations.
type DeltaTarget interface {
	ComputeSignature(relPath string, fileSize int64) (Signature, error)
	ApplyDelta(basisRelPath, tempRelPath string, ops []DeltaOp) (int64, error)
}

// SubtreeWalker is implemented by endpoints that support walking a subtree
// relative to the endpoint root (e.g. for building destination indexes).
type SubtreeWalker interface {
	WalkSubtree(subDir string, fn func(entry FileEntry) error) error
}

// PathResolver is implemented by endpoints that can resolve a relative path
// to an absolute filesystem path (local endpoints only).
type PathResolver interface {
	AbsPath(relPath string) string
}

// BatchWriteEntry describes a single file in a batch write request.
type BatchWriteEntry struct {
	ModTime time.Time
	AccTime time.Time
	RelPath string
	Data    []byte
	Perm    os.FileMode
	Mode    os.FileMode
	UID     uint32
	GID     uint32
}

// BatchWriteRequest is a batch of small files to write atomically.
type BatchWriteRequest struct {
	Entries []BatchWriteEntry
	Opts    MetadataOpts
}

// BatchWriteResult is the per-file result of a batch write.
type BatchWriteResult struct {
	RelPath string
	Error   string
	OK      bool
}

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

// Reader provides read-only filesystem operations relative to a root.
type Reader interface {
	// Walk recursively walks the tree rooted at the endpoint, calling fn for
	// each entry. relPath is relative to the endpoint root.
	Walk(fn func(entry FileEntry) error) error

	// Stat returns metadata for a single relative path.
	Stat(relPath string) (FileEntry, error)

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

// Writer provides write-only filesystem operations relative to a root.
type Writer interface {
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
}

// ReadWriter provides full read+write filesystem operations.
// Destinations need both: reads for delta basis, dst walking, skip detection;
// writes for the actual copy.
type ReadWriter interface {
	Reader
	Writer
}
