package engine

import "time"

// FileType identifies the kind of filesystem entry.
type FileType int

const (
	Regular FileType = iota
	Dir
	Symlink
	Hardlink
)

// DevIno uniquely identifies an inode for hardlink detection.
type DevIno struct {
	Dev uint64
	Ino uint64
}

// Chunk represents a portion of a large file.
type Chunk struct {
	Offset int64
	Length int64
}

// FileTask describes a single copy operation.
type FileTask struct {
	SrcPath    string    // 16 bytes (string header)
	DstPath    string    // 16 bytes
	LinkTarget string    // 16 bytes (for symlinks)
	ModTime    time.Time // 24 bytes
	AccTime    time.Time // 24 bytes
	Segments   []Segment // 24 bytes (slice header) — sparse file layout
	Chunks     []Chunk   // 24 bytes (slice header) — for large file splitting
	DevIno     DevIno    // 16 bytes
	Size       int64     // 8 bytes
	Mode       uint32    // 4 bytes
	UID        uint32    // 4 bytes
	GID        uint32    // 4 bytes
	Type       FileType  // int-sized
}
