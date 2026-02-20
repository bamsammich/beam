package stats

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectorConcurrent(t *testing.T) {
	c := &Collector{}
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
