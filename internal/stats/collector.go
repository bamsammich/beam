package stats

import (
	"fmt"
	"sync/atomic"
)

// Collector tracks copy operation statistics using lock-free atomic counters.
type Collector struct {
	filesScanned    atomic.Int64
	filesCopied     atomic.Int64
	filesFailed     atomic.Int64
	filesSkipped    atomic.Int64
	bytesCopied     atomic.Int64
	dirsCreated     atomic.Int64
	hardlinksCreated atomic.Int64
}

// Snapshot is a point-in-time read of all counters.
type Snapshot struct {
	FilesScanned     int64
	FilesCopied      int64
	FilesFailed      int64
	FilesSkipped     int64
	BytesCopied      int64
	DirsCreated      int64
	HardlinksCreated int64
}

func (c *Collector) AddFilesScanned(n int64)     { c.filesScanned.Add(n) }
func (c *Collector) AddFilesCopied(n int64)       { c.filesCopied.Add(n) }
func (c *Collector) AddFilesFailed(n int64)       { c.filesFailed.Add(n) }
func (c *Collector) AddFilesSkipped(n int64)      { c.filesSkipped.Add(n) }
func (c *Collector) AddBytesCopied(n int64)       { c.bytesCopied.Add(n) }
func (c *Collector) AddDirsCreated(n int64)       { c.dirsCreated.Add(n) }
func (c *Collector) AddHardlinksCreated(n int64)  { c.hardlinksCreated.Add(n) }

// Snapshot returns a consistent point-in-time read of all counters.
func (c *Collector) Snapshot() Snapshot {
	return Snapshot{
		FilesScanned:     c.filesScanned.Load(),
		FilesCopied:      c.filesCopied.Load(),
		FilesFailed:      c.filesFailed.Load(),
		FilesSkipped:     c.filesSkipped.Load(),
		BytesCopied:      c.bytesCopied.Load(),
		DirsCreated:      c.dirsCreated.Load(),
		HardlinksCreated: c.hardlinksCreated.Load(),
	}
}

func (s Snapshot) String() string {
	return fmt.Sprintf(
		"scanned=%d copied=%d failed=%d skipped=%d bytes=%d dirs=%d hardlinks=%d",
		s.FilesScanned, s.FilesCopied, s.FilesFailed, s.FilesSkipped,
		s.BytesCopied, s.DirsCreated, s.HardlinksCreated,
	)
}

// FormatBytes returns a human-readable byte count.
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
