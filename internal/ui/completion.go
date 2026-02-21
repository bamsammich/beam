package ui

import (
	"fmt"

	"github.com/bamsammich/beam/internal/stats"
)

// completionSummary builds a final summary line from a snapshot.
// Format: done ✓  files 48,917  size 2.1 GB  avg 641 MB/s  time 3m 17s  errors 0
func completionSummary(snap stats.Snapshot) string {
	avgSpeed := 0.0
	if snap.Elapsed.Seconds() > 0 {
		avgSpeed = float64(snap.BytesCopied) / snap.Elapsed.Seconds()
	}

	icon := "\u2713"
	if snap.FilesFailed > 0 {
		icon = "\u2717"
	}

	base := fmt.Sprintf("done %s  files %s  size %s  avg %s  time %s",
		icon,
		FormatCount(snap.FilesCopied),
		FormatBytes(snap.BytesCopied),
		FormatRate(avgSpeed),
		FormatDuration(snap.Elapsed),
	)

	if snap.FilesVerified > 0 || snap.FilesVerifyFailed > 0 {
		base += fmt.Sprintf("  verified %s", FormatCount(snap.FilesVerified))
	}

	base += fmt.Sprintf("  errors %d", snap.FilesFailed+snap.FilesVerifyFailed)

	return base
}
