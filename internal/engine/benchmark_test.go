package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunBenchmark(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	srcDir := filepath.Join(dir, "src")
	dstDir := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(srcDir, 0755))

	// Create a test file in the source directory.
	testFile := filepath.Join(srcDir, "testdata.bin")
	data := make([]byte, 1<<20) // 1 MB
	require.NoError(t, os.WriteFile(testFile, data, 0644))

	result, err := RunBenchmark(context.Background(), srcDir, dstDir)
	require.NoError(t, err)

	assert.Greater(t, result.ReadBytesPerSec, float64(0))
	assert.Greater(t, result.WriteBytesPerSec, float64(0))
	assert.Positive(t, result.SuggestedWorkers)
}

func TestRunBenchmark_EmptySource(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	srcDir := filepath.Join(dir, "empty")
	dstDir := filepath.Join(dir, "dst")
	require.NoError(t, os.MkdirAll(srcDir, 0755))

	_, err := RunBenchmark(context.Background(), srcDir, dstDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no readable files")
}

func TestSuggestWorkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		readBPS  float64
		writeBPS float64
		minW     int
		maxW     int
	}{
		{"NVMe", 3e9, 2.5e9, 4, 32},
		{"SSD", 500e6, 400e6, 2, 16},
		{"HDD", 100e6, 80e6, 1, 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := suggestWorkers(tc.readBPS, tc.writeBPS)
			assert.GreaterOrEqual(t, w, tc.minW)
			assert.LessOrEqual(t, w, tc.maxW)
		})
	}
}

func TestFormatBenchmark(t *testing.T) {
	t.Parallel()

	result := BenchmarkResult{
		ReadBytesPerSec:  2.1e9,
		WriteBytesPerSec: 1.8e9,
		SuggestedWorkers: 24,
	}
	s := FormatBenchmark(result)
	assert.Contains(t, s, "read 2.1 GB/s")
	assert.Contains(t, s, "write 1.8 GB/s")
	assert.Contains(t, s, "suggested workers 24")
}
