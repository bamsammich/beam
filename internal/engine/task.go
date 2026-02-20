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
	SrcPath    string
	DstPath    string
	Type       FileType
	Size       int64
	Mode       uint32
	Uid        uint32
	Gid        uint32
	ModTime    time.Time
	AccTime    time.Time
	DevIno     DevIno
	LinkTarget string    // for symlinks
	Segments   []Segment // sparse file layout
	Chunks     []Chunk   // for large file splitting
}
