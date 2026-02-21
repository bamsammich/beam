package ui

import (
	"fmt"
	"io"
	"time"

	"github.com/bamsammich/beam/internal/stats"
)

// plainPresenter outputs one line per completed file to stdout,
// and periodic progress to stderr when not a TTY.
type plainPresenter struct {
	w       io.Writer
	errW    io.Writer
	stats   *stats.Collector
	dstRoot string
}

func (p *plainPresenter) Run(events <-chan Event) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			p.handleEvent(ev)
		case <-ticker.C:
			p.printProgress()
		}
	}
}

func (p *plainPresenter) handleEvent(ev Event) {
	path := StripRoot(p.dstRoot, ev.Path)
	switch ev.Type {
	case ScanComplete:
		p.stats.SetTotals(ev.Total, ev.TotalSize)
	case FileCompleted:
		speed := p.stats.RollingSpeed(5)
		fmt.Fprintf(p.w, "%s  %s  %s\n", path, FormatBytes(ev.Size), FormatRate(speed))
	case FileFailed:
		errMsg := "error"
		if ev.Error != nil {
			errMsg = ev.Error.Error()
		}
		fmt.Fprintf(p.w, "%s  %s  %s\n", path, FormatBytes(ev.Size), errMsg)
	case FileSkipped:
		fmt.Fprintf(p.w, "%s  skipped\n", path)
	case DeleteFile:
		fmt.Fprintf(p.w, "delete: %s\n", path)
	case VerifyStarted:
		fmt.Fprintln(p.w, "verifying...")
	case VerifyFailed:
		fmt.Fprintf(p.w, "MISMATCH: %s\n", path)
	case VerifyOK:
		// silent in plain mode
	}
}

func (p *plainPresenter) printProgress() {
	snap := p.stats.Snapshot()
	if snap.BytesTotal > 0 {
		pct := float64(snap.BytesCopied) / float64(snap.BytesTotal) * 100
		speed := p.stats.RollingSpeed(10)
		eta := p.stats.ETA()
		fmt.Fprintf(p.errW, "progress: %.0f%% %s/%s %s/%s files %s eta %s\n",
			pct,
			FormatBytes(snap.BytesCopied), FormatBytes(snap.BytesTotal),
			FormatCount(snap.FilesCopied), FormatCount(snap.FilesTotal),
			FormatRate(speed),
			FormatETA(eta),
		)
	} else {
		fmt.Fprintf(p.errW, "progress: %s copied %s files\n",
			FormatBytes(snap.BytesCopied),
			FormatCount(snap.FilesCopied),
		)
	}
}

func (p *plainPresenter) Summary() string {
	return CompletionSummary(p.stats.Snapshot())
}
