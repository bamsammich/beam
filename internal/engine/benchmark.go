package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// BenchmarkResult holds throughput measurements.
type BenchmarkResult struct {
	ReadBytesPerSec  float64
	WriteBytesPerSec float64
	SuggestedWorkers int
}

// RunBenchmark measures source read and destination write throughput.
// It reads an existing file from srcDir and writes a temp file to dstDir.
func RunBenchmark(ctx context.Context, srcDir, dstDir string) (BenchmarkResult, error) {
	var result BenchmarkResult

	readSpeed, err := benchRead(ctx, srcDir)
	if err != nil {
		return result, fmt.Errorf("read benchmark: %w", err)
	}
	result.ReadBytesPerSec = readSpeed

	writeSpeed, err := benchWrite(ctx, dstDir)
	if err != nil {
		return result, fmt.Errorf("write benchmark: %w", err)
	}
	result.WriteBytesPerSec = writeSpeed

	result.SuggestedWorkers = suggestWorkers(readSpeed, writeSpeed)
	return result, nil
}

const benchSize = 64 * 1024 * 1024 // 64 MB

// findBenchFile walks srcDir to find a suitable file for benchmarking.
// Prefers files >= benchSize; falls back to any non-empty file.
//
//nolint:revive // cognitive-complexity: WalkDir callback with early-return guards — irreducible for file search
func findBenchFile(ctx context.Context, srcDir string) (string, error) {
	var target string
	err := filepath.WalkDir(srcDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return nil //nolint:nilerr // skip files we can't stat
		}
		if info.Size() >= benchSize {
			target = path
			return filepath.SkipAll
		}
		if target == "" && info.Size() > 0 {
			target = path
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if target == "" {
		return "", fmt.Errorf("no readable files in %s", srcDir)
	}
	return target, nil
}

// benchRead finds a file in srcDir and reads up to benchSize bytes, measuring throughput.
func benchRead(ctx context.Context, srcDir string) (float64, error) {
	target, err := findBenchFile(ctx, srcDir)
	if err != nil {
		return 0, err
	}

	f, err := os.Open(target)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	buf := make([]byte, 1<<20) // 1 MB read buffer
	var total int64
	start := time.Now()
	for total < benchSize {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		n, readErr := f.Read(buf)
		total += int64(n)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
	}
	elapsed := time.Since(start)
	if elapsed == 0 {
		elapsed = time.Microsecond
	}
	return float64(total) / elapsed.Seconds(), nil
}

// benchWrite creates a temp file on dstDir, writes benchSize bytes of zeros,
// fsyncs, and measures throughput.
func benchWrite(_ context.Context, dstDir string) (float64, error) {
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return 0, err
	}

	f, err := os.CreateTemp(dstDir, ".beam-bench-*")
	if err != nil {
		return 0, err
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	defer f.Close()

	buf := make([]byte, 1<<20) // 1 MB write buffer (zeros)
	var total int64
	start := time.Now()
	for total < benchSize {
		n, writeErr := f.Write(buf)
		total += int64(n)
		if writeErr != nil {
			return 0, writeErr
		}
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	if elapsed == 0 {
		elapsed = time.Microsecond
	}
	return float64(total) / elapsed.Seconds(), nil
}

// suggestWorkers returns a worker count based on measured throughput.
func suggestWorkers(readBPS, writeBPS float64) int {
	// Use the slower of read/write as the bottleneck indicator.
	bottleneck := readBPS
	if writeBPS < bottleneck {
		bottleneck = writeBPS
	}

	cpus := runtime.NumCPU()

	switch {
	case bottleneck >= 2e9: // >= 2 GB/s → NVMe
		return min(cpus*2, 32)
	case bottleneck >= 200e6: // >= 200 MB/s → SSD
		return min(cpus, 16)
	default: // HDD
		return min(4, cpus)
	}
}

// FormatBenchmark formats a BenchmarkResult for display.
func FormatBenchmark(r BenchmarkResult) string {
	return fmt.Sprintf("benchmark: read %s/s  write %s/s  suggested workers %d",
		formatBytes(r.ReadBytesPerSec), formatBytes(r.WriteBytesPerSec), r.SuggestedWorkers)
}

func formatBytes(b float64) string {
	switch {
	case b >= 1e9:
		return fmt.Sprintf("%.1f GB", b/1e9)
	case b >= 1e6:
		return fmt.Sprintf("%.0f MB", b/1e6)
	case b >= 1e3:
		return fmt.Sprintf("%.0f KB", b/1e3)
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}
