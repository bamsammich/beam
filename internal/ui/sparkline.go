package ui

// Sparkline renders a slice of float64 values as Unicode block characters.
// The output is exactly width runes wide. Values are normalized to the max
// value in the input slice.
func Sparkline(data []float64, width int) string {
	if width <= 0 {
		return ""
	}

	blocks := []rune("▁▂▃▄▅▆▇█")

	// Take the last `width` samples, or pad left with zeros.
	samples := make([]float64, width)
	if len(data) >= width {
		copy(samples, data[len(data)-width:])
	} else {
		offset := width - len(data)
		copy(samples[offset:], data)
	}

	// Find max.
	maxVal := 0.0
	for _, v := range samples {
		if v > maxVal {
			maxVal = v
		}
	}

	out := make([]rune, width)
	for i, v := range samples {
		if maxVal <= 0 || v <= 0 {
			out[i] = blocks[0]
			continue
		}
		idx := int(v / maxVal * float64(len(blocks)-1))
		if idx >= len(blocks) {
			idx = len(blocks) - 1
		}
		out[i] = blocks[idx]
	}
	return string(out)
}
