package ui

import "github.com/bamsammich/beam/internal/event"

// Re-export event types for convenience.
const (
	ScanStarted     = event.ScanStarted
	ScanComplete    = event.ScanComplete
	FileStarted     = event.FileStarted
	FileProgress    = event.FileProgress
	FileCompleted   = event.FileCompleted
	FileFailed      = event.FileFailed
	FileSkipped     = event.FileSkipped
	DirCreated      = event.DirCreated
	HardlinkCreated = event.HardlinkCreated
	DeleteFile      = event.DeleteFile
	VerifyStarted   = event.VerifyStarted
	VerifyOK        = event.VerifyOK
	VerifyFailed    = event.VerifyFailed
)
