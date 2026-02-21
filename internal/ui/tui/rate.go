package tui

import (
	"fmt"
	"strings"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/ui"
)

type rateView struct {
	busyWorkers map[int]bool
}

func newRateView() rateView {
	return rateView{
		busyWorkers: make(map[int]bool),
	}
}

func (r *rateView) handleEvent(ev event.Event) {
	switch ev.Type {
	case event.FileStarted:
		r.busyWorkers[ev.WorkerID] = true
	case event.FileCompleted, event.FileFailed, event.DirCreated, event.HardlinkCreated:
		delete(r.busyWorkers, ev.WorkerID)
	}
}

func (r *rateView) view(width, height int, snap stats.Snapshot, collector *stats.Collector, totalWorkers int) string {
	if width < 20 {
		width = 20
	}

	var b strings.Builder

	// Big throughput number.
	speed := collector.RollingSpeed(5)
	speedStr := styleBigNumber.Render(ui.FormatRate(speed))
	b.WriteString("  " + speedStr)
	b.WriteByte('\n')
	b.WriteByte('\n')

	// Full-width sparkline (60-second history).
	sparkWidth := width - 4
	if sparkWidth < 10 {
		sparkWidth = 10
	}
	sparkData := collector.SparklineData(sparkWidth)
	spark := ui.Sparkline(sparkData, sparkWidth)
	b.WriteString("  " + styleSparkline.Render(spark))
	b.WriteByte('\n')
	b.WriteByte('\n')

	// Stats cells: files/sec + bytes/sec.
	fps := collector.RollingFilesPerSec(5)
	fpsStr := fmt.Sprintf("%s files/s", ui.FormatCount(int64(fps)))
	bpsStr := ui.FormatRate(speed)
	filesStr := fmt.Sprintf("%s / %s files",
		ui.FormatCount(snap.FilesCopied),
		ui.FormatCount(snap.FilesTotal))

	statLine := fmt.Sprintf("  %s   %s   %s",
		styleFileSpeed.Render(fpsStr),
		styleFileSpeed.Render(bpsStr),
		styleFileSize.Render(filesStr),
	)
	b.WriteString(statLine)
	b.WriteByte('\n')
	b.WriteByte('\n')

	// Worker grid.
	b.WriteString("  " + styleDivider.Render("workers") + "  ")
	b.WriteString(r.renderWorkerGrid(totalWorkers))
	b.WriteByte('\n')

	return b.String()
}

func (r *rateView) renderWorkerGrid(total int) string {
	var b strings.Builder
	for i := range total {
		if r.busyWorkers[i] {
			b.WriteString(styleWorkerBusy.Render("▪"))
		} else {
			b.WriteString(styleWorkerIdle.Render("□"))
		}
	}
	return b.String()
}
