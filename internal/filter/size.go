package filter

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseSize parses a human-readable size string into bytes.
// Supports: 100, 100B, 100K, 100M, 100G, 100T (case-insensitive).
// Uses powers of 1024 (matching rsync behavior).
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Determine the multiplier suffix.
	multiplier := int64(1)
	numStr := s

	last := strings.ToUpper(s[len(s)-1:])
	switch last {
	case "B":
		multiplier = 1
		numStr = s[:len(s)-1]
	case "K":
		multiplier = 1024
		numStr = s[:len(s)-1]
	case "M":
		multiplier = 1024 * 1024
		numStr = s[:len(s)-1]
	case "G":
		multiplier = 1024 * 1024 * 1024
		numStr = s[:len(s)-1]
	case "T":
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = s[:len(s)-1]
	default:
		// No suffix, try parsing as plain number.
	}

	if numStr == "" {
		return 0, fmt.Errorf("invalid size: %q", s)
	}

	// Try integer first, then float.
	if n, err := strconv.ParseInt(numStr, 10, 64); err == nil {
		return n * multiplier, nil
	}

	f, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size: %q", s)
	}

	return int64(f * float64(multiplier)), nil
}
