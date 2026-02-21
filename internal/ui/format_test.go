package ui

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatRate(t *testing.T) {
	tests := []struct {
		input float64
		want  string
	}{
		{0, "0 B/s"},
		{-1, "0 B/s"},
		{512, "512 B/s"},
		{1024, "1.00 KB/s"},
		{1.5 * 1024 * 1024, "1.50 MB/s"},
		{2.5 * 1024 * 1024 * 1024, "2.50 GB/s"},
		{100 * 1024, "100 KB/s"},
		{15 * 1024, "15.0 KB/s"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatRate(tt.input))
		})
	}
}

func TestFormatETA(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "--"},
		{-1 * time.Second, "--"},
		{30 * time.Second, "30s"},
		{90 * time.Second, "1m 30s"},
		{3661 * time.Second, "1h 01m 01s"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatETA(tt.input))
		})
	}
}

func TestFormatCount(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1000000, "1,000,000"},
		{14302, "14,302"},
		{-1000, "-1,000"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatCount(tt.input))
		})
	}
}

func TestProgressBar(t *testing.T) {
	bar := ProgressBar(0.5, 10)
	assert.Equal(t, "▪▪▪▪▪□□□□□", bar)

	bar = ProgressBar(0, 10)
	assert.Equal(t, "□□□□□□□□□□", bar)

	bar = ProgressBar(1.0, 10)
	assert.Equal(t, "▪▪▪▪▪▪▪▪▪▪", bar)

	// Edge cases.
	assert.Equal(t, "", ProgressBar(0.5, 0))
	assert.Equal(t, "▪▪▪▪▪▪▪▪▪▪", ProgressBar(1.5, 10)) // clamp
}

func TestWorkerIndicator(t *testing.T) {
	assert.Equal(t, "▪▪▪□□□", WorkerIndicator(3, 6))
	assert.Equal(t, "□□□□", WorkerIndicator(0, 4))
	assert.Equal(t, "▪▪▪▪", WorkerIndicator(4, 4))
}

func TestFormatDuration(t *testing.T) {
	assert.Equal(t, "0s", FormatDuration(0))
	assert.Equal(t, "30s", FormatDuration(30*time.Second))
	assert.Equal(t, "3m 17s", FormatDuration(3*time.Minute+17*time.Second))
	assert.Equal(t, "1h 02m 03s", FormatDuration(1*time.Hour+2*time.Minute+3*time.Second))
}
