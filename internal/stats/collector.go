package stats

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const ringSize = 60

// Collector tracks copy operation statistics using lock-free atomic counters.
type Collector struct {
	filesScanned     atomic.Int64
	filesCopied      atomic.Int64
	filesFailed      atomic.Int64
	filesSkipped     atomic.Int64
	bytesCopied      atomic.Int64
	dirsCreated      atomic.Int64
	hardlinksCreated atomic.Int64
	bytesTotal        atomic.Int64
	filesTotal        atomic.Int64
	filesVerified     atomic.Int64
	filesVerifyFailed atomic.Int64
	startTime         time.Time

	// Ring buffer — written only by presenter's Tick(), not workers.
	mu          sync.Mutex
	throughput  [ringSize]int64 // bytes delta per second
	filesPerSec [ringSize]int64 // files delta per second
	ringIdx     int
	ringCount   int // how many samples have been written (capped at ringSize)
	lastBytes   int64
	lastFiles   int64
}

// NewCollector creates a Collector with startTime set to now.
func NewCollector() *Collector {
	return &Collector{startTime: time.Now()}
}

// SetTotals records scan totals (called once when scan completes).
func (c *Collector) SetTotals(files, bytes int64) {
	c.filesTotal.Store(files)
	c.bytesTotal.Store(bytes)
}

// AddFilesTotal atomically increments the total file count (used during scanning).
func (c *Collector) AddFilesTotal(n int64) { c.filesTotal.Add(n) }

// AddBytesTotal atomically increments the total byte count (used during scanning).
func (c *Collector) AddBytesTotal(n int64) { c.bytesTotal.Add(n) }

// Snapshot is a point-in-time read of all counters.
type Snapshot struct {
	FilesScanned     int64
	FilesCopied      int64
	FilesFailed      int64
	FilesSkipped     int64
	BytesCopied      int64
	DirsCreated      int64
	HardlinksCreated int64
	BytesTotal        int64
	FilesTotal        int64
	FilesVerified     int64
	FilesVerifyFailed int64
	Elapsed           time.Duration
}

func (c *Collector) AddFilesScanned(n int64)    { c.filesScanned.Add(n) }
func (c *Collector) AddFilesCopied(n int64)      { c.filesCopied.Add(n) }
func (c *Collector) AddFilesFailed(n int64)      { c.filesFailed.Add(n) }
func (c *Collector) AddFilesSkipped(n int64)     { c.filesSkipped.Add(n) }
func (c *Collector) AddBytesCopied(n int64)      { c.bytesCopied.Add(n) }
func (c *Collector) AddDirsCreated(n int64)      { c.dirsCreated.Add(n) }
func (c *Collector) AddHardlinksCreated(n int64) { c.hardlinksCreated.Add(n) }
func (c *Collector) AddFilesVerified(n int64)     { c.filesVerified.Add(n) }
func (c *Collector) AddFilesVerifyFailed(n int64)  { c.filesVerifyFailed.Add(n) }

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
		BytesTotal:        c.bytesTotal.Load(),
		FilesTotal:        c.filesTotal.Load(),
		FilesVerified:     c.filesVerified.Load(),
		FilesVerifyFailed: c.filesVerifyFailed.Load(),
		Elapsed:           c.Elapsed(),
	}
}

// Tick snapshots byte/file deltas into the ring buffer. Called 1/sec by the presenter.
func (c *Collector) Tick() {
	currentBytes := c.bytesCopied.Load()
	currentFiles := c.filesCopied.Load()

	c.mu.Lock()
	defer c.mu.Unlock()

	bytesDelta := currentBytes - c.lastBytes
	filesDelta := currentFiles - c.lastFiles
	c.lastBytes = currentBytes
	c.lastFiles = currentFiles

	c.throughput[c.ringIdx] = bytesDelta
	c.filesPerSec[c.ringIdx] = filesDelta
	c.ringIdx = (c.ringIdx + 1) % ringSize
	if c.ringCount < ringSize {
		c.ringCount++
	}
}

// RollingSpeed returns average bytes/sec over the last n seconds of samples.
func (c *Collector) RollingSpeed(seconds int) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rollingAvg(c.throughput[:], seconds)
}

// RollingFilesPerSec returns average files/sec over the last n seconds.
func (c *Collector) RollingFilesPerSec(seconds int) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rollingAvg(c.filesPerSec[:], seconds)
}

func (c *Collector) rollingAvg(buf []int64, n int) float64 {
	count := n
	if count > c.ringCount {
		count = c.ringCount
	}
	if count == 0 {
		return 0
	}
	var sum int64
	for i := range count {
		idx := (c.ringIdx - 1 - i + ringSize) % ringSize
		sum += buf[idx]
	}
	return float64(sum) / float64(count)
}

// SparklineData returns the last n bytes/sec samples for rendering.
func (c *Collector) SparklineData(n int) []float64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := n
	if count > c.ringCount {
		count = c.ringCount
	}
	if count == 0 {
		return nil
	}

	data := make([]float64, count)
	for i := range count {
		// oldest first
		idx := (c.ringIdx - count + i + ringSize) % ringSize
		data[i] = float64(c.throughput[idx])
	}
	return data
}

// ETA estimates remaining time based on rolling speed and remaining bytes.
func (c *Collector) ETA() time.Duration {
	speed := c.RollingSpeed(10)
	if speed <= 0 {
		return 0
	}
	remaining := c.bytesTotal.Load() - c.bytesCopied.Load()
	if remaining <= 0 {
		return 0
	}
	return time.Duration(float64(remaining)/speed) * time.Second
}

// Elapsed returns time since collector creation.
func (c *Collector) Elapsed() time.Duration {
	return time.Since(c.startTime)
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
