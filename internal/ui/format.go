package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/bamsammich/beam/internal/stats"
)

// FormatRate formats a bytes-per-second rate as a human-readable string.
func FormatRate(bytesPerSec float64) string {
	if bytesPerSec <= 0 {
		return "0 B/s"
	}
	units := []string{"B/s", "KB/s", "MB/s", "GB/s", "TB/s"}
	val := bytesPerSec
	for _, u := range units {
		if val < 1024 {
			if val < 10 {
				return fmt.Sprintf("%.2f %s", val, u)
			}
			if val < 100 {
				return fmt.Sprintf("%.1f %s", val, u)
			}
			return fmt.Sprintf("%.0f %s", val, u)
		}
		val /= 1024
	}
	return fmt.Sprintf("%.1f PB/s", val)
}

// FormatETA formats a duration as a human-readable ETA string.
func FormatETA(d time.Duration) string {
	if d <= 0 {
		return "--"
	}
	d = d.Round(time.Second)

	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	if h > 0 {
		return fmt.Sprintf("%dh %02dm %02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// FormatCount formats an integer with comma separators.
func FormatCount(n int64) string {
	if n < 0 {
		return "-" + FormatCount(-n)
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	remainder := len(s) % 3
	if remainder > 0 {
		b.WriteString(s[:remainder])
	}
	for i := remainder; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

// ProgressBar renders a progress bar of the given width using ▪/□ characters.
func ProgressBar(pct float64, width int) string {
	if width <= 0 {
		return ""
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 1 {
		pct = 1
	}

	filled := int(pct * float64(width))
	if filled > width {
		filled = width
	}

	var b strings.Builder
	for range filled {
		b.WriteRune('\u25aa') // ▪ (filled)
	}
	for range width - filled {
		b.WriteRune('\u25a1') // □ (empty)
	}
	return b.String()
}

// WorkerIndicator renders a worker activity display.
// busy is the number of active workers, total is the total count.
func WorkerIndicator(busy, total int) string {
	var b strings.Builder
	for i := range total {
		if i < busy {
			b.WriteRune('▪')
		} else {
			b.WriteRune('□')
		}
	}
	return b.String()
}

// FormatBytes wraps stats.FormatBytes for UI use.
func FormatBytes(b int64) string {
	return stats.FormatBytes(b)
}

// FormatDuration formats elapsed time concisely.
func FormatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	if h > 0 {
		return fmt.Sprintf("%dh %02dm %02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
