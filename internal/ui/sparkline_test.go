package ui

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSparklineAllZeros(t *testing.T) {
	result := Sparkline([]float64{0, 0, 0, 0, 0}, 5)
	assert.Equal(t, "▁▁▁▁▁", result)
}

func TestSparklineSingleSample(t *testing.T) {
	result := Sparkline([]float64{100}, 5)
	// 1 value, padded left with 4 zeros.
	assert.Len(t, []rune(result), 5)
	runes := []rune(result)
	assert.Equal(t, '▁', runes[0]) // zero padding
	assert.Equal(t, '█', runes[4]) // the single sample is max
}

func TestSparklineNormalRange(t *testing.T) {
	data := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	result := Sparkline(data, 8)
	runes := []rune(result)
	assert.Len(t, runes, 8)
	// First should be the smallest block, last should be the largest.
	assert.Equal(t, '▁', runes[0])
	assert.Equal(t, '█', runes[7])
}

func TestSparklineAllSame(t *testing.T) {
	data := []float64{5, 5, 5, 5}
	result := Sparkline(data, 4)
	// When all values are the same, they all map to max (█).
	runes := []rune(result)
	for _, r := range runes {
		assert.Equal(t, '█', r)
	}
}

func TestSparklineZeroWidth(t *testing.T) {
	assert.Equal(t, "", Sparkline([]float64{1, 2, 3}, 0))
}

func TestSparklineTruncation(t *testing.T) {
	// More data than width: takes last `width` samples.
	data := []float64{10, 20, 30, 40, 50}
	result := Sparkline(data, 3)
	assert.Len(t, []rune(result), 3)
}
