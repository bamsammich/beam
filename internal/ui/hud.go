package ui

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/bamsammich/beam/internal/stats"
)

// ANSI escape sequences.
const (
	ansiDim   = "\033[2m"
	ansiBold  = "\033[1m"
	ansiReset = "\033[0m"
)

// hudPresenter provides a rich TTY display with a scrolling feed of completed
// files and a 2-line HUD that redraws in place.
type hudPresenter struct {
	w         io.Writer
	stats     *stats.Collector
	forceFeed bool
	forceRate bool
	workers   int
	dstRoot   string // destination root, stripped from displayed paths

	// Internal state.
	hudDrawn     bool
	hudLineCount int // actual number of lines in the last HUD draw
	rateMode     bool
	rateSwitched bool // whether we've printed the switch notice
	busyWorkers  map[int]bool
	lastHUDDraw  time.Time
}

const (
	rateThreshHigh   = 200.0
	rateThreshLow    = 100.0
	sparklineWidth   = 20
	progressBarWidth = 20
	hudMinInterval   = 50 * time.Millisecond // don't redraw faster than this
)

func (p *hudPresenter) Run(events <-chan Event) error {
	p.busyWorkers = make(map[int]bool)

	if p.forceRate {
		p.rateMode = true
	}

	// Fire first tick quickly to seed the ring buffer with initial speed data,
	// then switch to 1s interval.
	secTicker := time.NewTicker(250 * time.Millisecond)
	defer secTicker.Stop()
	firstTickDone := false

	// Redraw ticker for when no events are flowing (e.g., large file copy).
	redrawTicker := time.NewTicker(100 * time.Millisecond)
	defer redrawTicker.Stop()

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				p.clearHUD()
				return nil
			}
			p.handleEvent(ev)
			p.maybeDrawHUD()

		case <-redrawTicker.C:
			p.maybeSwitch()
			p.drawHUD()

		case <-secTicker.C:
			p.stats.Tick()
			if !firstTickDone {
				firstTickDone = true
				secTicker.Reset(1 * time.Second)
			}
		}
	}
}

func (p *hudPresenter) handleEvent(ev Event) {
	switch ev.Type {
	case ScanComplete:
		p.stats.SetTotals(ev.Total, ev.TotalSize)

	case FileStarted:
		p.busyWorkers[ev.WorkerID] = true

	case FileCompleted:
		delete(p.busyWorkers, ev.WorkerID)
		if !p.rateMode {
			p.clearHUD()
			p.printFileCompleted(ev)
			p.drawHUD() // always redraw HUD after feed line
		}

	case FileFailed:
		delete(p.busyWorkers, ev.WorkerID)
		if !p.rateMode {
			p.clearHUD()
			p.printFileFailed(ev)
			p.drawHUD()
		}

	case FileSkipped:
		if !p.rateMode {
			p.clearHUD()
			p.printFileSkipped(ev)
			p.drawHUD()
		}

	case DeleteFile:
		if !p.rateMode {
			p.clearHUD()
			fmt.Fprintf(p.w, "\u00d7  %s  %s(would delete)%s\n",
				p.styledPath(ev.Path), ansiDim, ansiReset)
			p.drawHUD()
		}

	case VerifyStarted:
		p.clearHUD()
		fmt.Fprintf(p.w, "%sverifying checksums...%s\n", ansiDim, ansiReset)

	case VerifyOK:
		// Only show in verbose / feed mode — dim output.

	case VerifyFailed:
		p.clearHUD()
		fmt.Fprintf(p.w, "\u2717  %s  CHECKSUM MISMATCH\n", p.styledPath(ev.Path))
		p.drawHUD()

	case DirCreated, HardlinkCreated:
		delete(p.busyWorkers, ev.WorkerID)
	}
}

func (p *hudPresenter) printFileCompleted(ev Event) {
	speed := p.stats.RollingSpeed(5)
	if speed > 0 {
		fmt.Fprintf(p.w, "\u2713  %s  %10s  %s\n",
			p.styledPath(ev.Path), FormatBytes(ev.Size), FormatRate(speed))
	} else {
		fmt.Fprintf(p.w, "\u2713  %s  %10s\n",
			p.styledPath(ev.Path), FormatBytes(ev.Size))
	}
}

func (p *hudPresenter) printFileFailed(ev Event) {
	errMsg := "error"
	if ev.Error != nil {
		errMsg = ev.Error.Error()
	}
	fmt.Fprintf(p.w, "\u2717  %s  %10s  %s\n",
		p.styledPath(ev.Path), FormatBytes(ev.Size), errMsg)
}

func (p *hudPresenter) printFileSkipped(ev Event) {
	fmt.Fprintf(p.w, "\u2013  %s  %10s  %sskipped%s\n",
		p.styledPath(ev.Path), FormatBytes(ev.Size), ansiDim, ansiReset)
}

func (p *hudPresenter) maybeSwitch() {
	if p.forceFeed || p.forceRate {
		return
	}

	fps := p.stats.RollingFilesPerSec(2)

	if !p.rateMode && fps > rateThreshHigh {
		p.rateMode = true
		if !p.rateSwitched {
			p.rateSwitched = true
			p.clearHUD()
			fmt.Fprintf(p.w, "\u21af rate view (%s files/s \u00b7 use --feed to see individual files)\n",
				FormatCount(int64(fps)))
		}
	} else if p.rateMode && fps < rateThreshLow {
		p.rateMode = false
	}
}

// maybeDrawHUD redraws the HUD if enough time has passed since the last draw.
func (p *hudPresenter) maybeDrawHUD() {
	now := time.Now()
	if now.Sub(p.lastHUDDraw) < hudMinInterval {
		return
	}
	p.drawHUD()
}

func (p *hudPresenter) drawHUD() {
	snap := p.stats.Snapshot()

	// Clear previous HUD if drawn.
	p.clearHUD()

	var pct float64
	if snap.BytesTotal > 0 {
		pct = float64(snap.BytesCopied) / float64(snap.BytesTotal)
	}

	speed := p.stats.RollingSpeed(10)
	eta := p.stats.ETA()

	lines := 0

	// Rate mode: extra files/s line above the main HUD.
	if p.rateMode {
		fps := p.stats.RollingFilesPerSec(5)
		sparkData := p.stats.SparklineData(sparklineWidth)
		spark := Sparkline(sparkData, sparklineWidth)
		fmt.Fprintf(p.w, "files/s  %s  %s/s   %s / %s done\n",
			spark, FormatCount(int64(fps)),
			FormatCount(snap.FilesCopied), FormatCount(snap.FilesTotal))
		lines++
	}

	// Line 1: throughput sparkline + speed + byte totals.
	sparkData := p.stats.SparklineData(sparklineWidth)
	spark := Sparkline(sparkData, sparklineWidth)
	fmt.Fprintf(p.w, "       %s   %s   %s / %s\n",
		spark, FormatRate(speed),
		FormatBytes(snap.BytesCopied), FormatBytes(snap.BytesTotal))
	lines++

	// Line 2: progress bar (▪/□) + files + eta.
	bar := ProgressBar(pct, progressBarWidth)
	fmt.Fprintf(p.w, " %3.0f%%  %s   %s / %s files   eta %s\n",
		pct*100, bar,
		FormatCount(snap.FilesCopied), FormatCount(snap.FilesTotal),
		FormatETA(eta))
	lines++

	p.hudDrawn = true
	p.hudLineCount = lines
	p.lastHUDDraw = time.Now()
}

func (p *hudPresenter) clearHUD() {
	if !p.hudDrawn {
		return
	}
	lines := p.hudLineCount
	if lines == 0 {
		lines = 2 // fallback
	}
	// Move cursor up N lines and clear to end of screen.
	fmt.Fprintf(p.w, "\033[%dA\033[J", lines)
	p.hudDrawn = false
}

func (p *hudPresenter) Summary() string {
	return CompletionSummary(p.stats.Snapshot())
}

// relPath strips the dstRoot prefix from an absolute path to produce a
// cleaner relative path for display. Falls back to the original path.
func (p *hudPresenter) relPath(path string) string {
	if p.dstRoot == "" {
		return path
	}
	rel, err := filepath.Rel(p.dstRoot, path)
	if err != nil {
		return path
	}
	return rel
}

// styledPath returns the path with the directory portion dimmed and the
// filename in normal weight, making the actual filename stand out.
func (p *hudPresenter) styledPath(path string) string {
	path = p.relPath(path)
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	if dir == "." || dir == "" {
		return base
	}
	return fmt.Sprintf("%s%s/%s%s", ansiDim, dir, ansiReset, base)
}

// truncPath shortens a path to fit within maxLen characters.
func truncPath(path string, maxLen int) string {
	if len(path) <= maxLen {
		return path
	}
	if maxLen <= 3 {
		return path[:maxLen]
	}
	return "..." + path[len(path)-maxLen+3:]
}

// StripRoot removes a root prefix from a path, returning a clean relative path.
// Exported for use by the plain presenter.
func StripRoot(root, path string) string {
	if root == "" {
		return path
	}
	// Ensure root ends with separator for clean stripping.
	if !strings.HasSuffix(root, string(filepath.Separator)) {
		root += string(filepath.Separator)
	}
	if strings.HasPrefix(path, root) {
		return path[len(root):]
	}
	return path
}
