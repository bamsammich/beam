package event

import "time"

// Type identifies the kind of event.
type Type int

const (
	ScanStarted     Type = iota + 1
	ScanComplete
	FileStarted
	FileProgress
	FileCompleted
	FileFailed
	FileSkipped
	DirCreated
	HardlinkCreated
	DeleteFile
)

var typeNames = [...]string{
	ScanStarted:     "ScanStarted",
	ScanComplete:    "ScanComplete",
	FileStarted:     "FileStarted",
	FileProgress:    "FileProgress",
	FileCompleted:   "FileCompleted",
	FileFailed:      "FileFailed",
	FileSkipped:     "FileSkipped",
	DirCreated:      "DirCreated",
	HardlinkCreated: "HardlinkCreated",
	DeleteFile:      "DeleteFile",
}

func (t Type) String() string {
	if int(t) < len(typeNames) {
		return typeNames[t]
	}
	return "Unknown"
}

// Event represents a single progress event from the engine.
type Event struct {
	Type      Type
	Timestamp time.Time
	Path      string // relative path
	Size      int64  // file size or bytes-so-far
	Total     int64  // total files (ScanComplete)
	TotalSize int64  // total bytes (ScanComplete)
	Error     error
	WorkerID  int
}
