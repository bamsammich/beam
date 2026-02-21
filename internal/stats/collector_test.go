package stats

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectorConcurrent(t *testing.T) {
	c := NewCollector()
	const goroutines = 100
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				c.AddFilesScanned(1)
				c.AddFilesCopied(1)
				c.AddFilesFailed(1)
				c.AddFilesSkipped(1)
				c.AddBytesCopied(256)
				c.AddDirsCreated(1)
				c.AddHardlinksCreated(1)
			}
		}()
	}
	wg.Wait()

	s := c.Snapshot()
	expected := int64(goroutines * opsPerGoroutine)
	assert.Equal(t, expected, s.FilesScanned)
	assert.Equal(t, expected, s.FilesCopied)
	assert.Equal(t, expected, s.FilesFailed)
	assert.Equal(t, expected, s.FilesSkipped)
	assert.Equal(t, expected*256, s.BytesCopied)
	assert.Equal(t, expected, s.DirsCreated)
	assert.Equal(t, expected, s.HardlinksCreated)
}

func TestSnapshotString(t *testing.T) {
	s := Snapshot{
		FilesScanned:     10,
		FilesCopied:      8,
		FilesFailed:      1,
		FilesSkipped:     1,
		BytesCopied:      4096,
		DirsCreated:      3,
		HardlinksCreated: 2,
	}
	expected := "scanned=10 copied=8 failed=1 skipped=1 bytes=4096 dirs=3 hardlinks=2"
	assert.Equal(t, expected, s.String())
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
		{1073741824, "1.0 GiB"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			require.Equal(t, tt.expected, FormatBytes(tt.input))
		})
	}
}

func TestNewCollector(t *testing.T) {
	c := NewCollector()
	assert.False(t, c.startTime.IsZero())
	assert.InDelta(t, 0, c.Elapsed().Seconds(), 1)
}

func TestSetTotals(t *testing.T) {
	c := NewCollector()
	c.SetTotals(100, 1024*1024)
	s := c.Snapshot()
	assert.Equal(t, int64(100), s.FilesTotal)
	assert.Equal(t, int64(1024*1024), s.BytesTotal)
}

func TestTickAndRollingSpeed(t *testing.T) {
	c := NewCollector()

	// Simulate 5 seconds of 1000 bytes/sec.
	for i := range 5 {
		c.AddBytesCopied(1000)
		c.AddFilesCopied(10)
		_ = i
		c.Tick()
	}

	speed := c.RollingSpeed(5)
	assert.InDelta(t, 1000.0, speed, 0.01)

	fps := c.RollingFilesPerSec(5)
	assert.InDelta(t, 10.0, fps, 0.01)
}

func TestRollingSpeedPartialWindow(t *testing.T) {
	c := NewCollector()

	// Only 2 samples.
	c.AddBytesCopied(500)
	c.Tick()
	c.AddBytesCopied(500)
	c.Tick()

	// Ask for 10 but only have 2.
	speed := c.RollingSpeed(10)
	assert.InDelta(t, 500.0, speed, 0.01)
}

func TestRollingSpeedNoSamples(t *testing.T) {
	c := NewCollector()
	assert.Equal(t, 0.0, c.RollingSpeed(5))
}

func TestSparklineData(t *testing.T) {
	c := NewCollector()

	for i := range 5 {
		c.AddBytesCopied(int64((i + 1) * 100))
		c.Tick()
	}

	data := c.SparklineData(5)
	require.Len(t, data, 5)
	// Each tick's delta: 100, 200, 300, 400, 500.
	assert.InDelta(t, 100, data[0], 0.01)
	assert.InDelta(t, 200, data[1], 0.01)
	assert.InDelta(t, 300, data[2], 0.01)
	assert.InDelta(t, 400, data[3], 0.01)
	assert.InDelta(t, 500, data[4], 0.01)
}

func TestSparklineDataNoSamples(t *testing.T) {
	c := NewCollector()
	assert.Nil(t, c.SparklineData(5))
}

func TestRingWraparound(t *testing.T) {
	c := NewCollector()

	// Fill past the ring buffer.
	for i := range ringSize + 10 {
		c.AddBytesCopied(int64(i + 1))
		c.Tick()
	}

	// Should still work, returning last ringSize samples.
	data := c.SparklineData(ringSize)
	require.Len(t, data, ringSize)
}

func TestETA(t *testing.T) {
	c := NewCollector()
	c.SetTotals(100, 10000)

	// Simulate copying 5000 bytes at 1000/sec.
	for range 5 {
		c.AddBytesCopied(1000)
		c.Tick()
	}

	eta := c.ETA()
	assert.InDelta(t, 5.0, eta.Seconds(), 1.0)
}

func TestETANoSpeed(t *testing.T) {
	c := NewCollector()
	c.SetTotals(100, 10000)
	assert.Equal(t, time.Duration(0), c.ETA())
}

func TestETAComplete(t *testing.T) {
	c := NewCollector()
	c.SetTotals(1, 1000)
	c.AddBytesCopied(1000)
	c.Tick()
	assert.Equal(t, time.Duration(0), c.ETA())
}

func TestSnapshotIncludesElapsed(t *testing.T) {
	c := NewCollector()
	time.Sleep(10 * time.Millisecond)
	s := c.Snapshot()
	assert.Greater(t, s.Elapsed, time.Duration(0))
}
