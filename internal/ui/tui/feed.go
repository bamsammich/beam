package tui

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/ui"
)

type inFlightEntry struct {
	path     string
	workerID int
	size     int64
	started  time.Time
	progress int64 // bytes copied so far
}

type completedEntry struct {
	path    string
	size    int64
	speed   float64
	skipped bool
	failed  bool
	errMsg  string
}

type errorEntry struct {
	path string
	size int64
	err  string
	time time.Time
}

type feedView struct {
	inFlight     map[int]*inFlightEntry // keyed by workerID
	completed    []completedEntry       // unbounded history
	errors       []errorEntry           // never evicted
	dstRoot      string
	scrollOffset int  // viewport offset into completed list
	autoScroll   bool // follow new entries
}

func newFeedView(dstRoot string) feedView {
	return feedView{
		inFlight:   make(map[int]*inFlightEntry),
		dstRoot:    dstRoot,
		autoScroll: true,
	}
}

func (f *feedView) handleEvent(ev event.Event) {
	switch ev.Type {
	case event.FileStarted:
		f.inFlight[ev.WorkerID] = &inFlightEntry{
			path:     ev.Path,
			workerID: ev.WorkerID,
			size:     ev.Size,
			started:  ev.Timestamp,
		}

	case event.FileProgress:
		if e, ok := f.inFlight[ev.WorkerID]; ok {
			e.progress = ev.Size
		}

	case event.FileCompleted:
		delete(f.inFlight, ev.WorkerID)
		f.addCompleted(completedEntry{
			path: ev.Path,
			size: ev.Size,
		})

	case event.FileFailed:
		delete(f.inFlight, ev.WorkerID)
		errMsg := "error"
		if ev.Error != nil {
			errMsg = ev.Error.Error()
		}
		f.addCompleted(completedEntry{
			path:   ev.Path,
			size:   ev.Size,
			failed: true,
			errMsg: errMsg,
		})
		f.errors = append(f.errors, errorEntry{
			path: ev.Path,
			size: ev.Size,
			err:  errMsg,
			time: ev.Timestamp,
		})

	case event.FileSkipped:
		f.addCompleted(completedEntry{
			path:    ev.Path,
			size:    ev.Size,
			skipped: true,
		})

	case event.VerifyFailed:
		f.errors = append(f.errors, errorEntry{
			path: ev.Path,
			err:  "CHECKSUM MISMATCH",
			time: ev.Timestamp,
		})

	case event.DirCreated, event.HardlinkCreated:
		delete(f.inFlight, ev.WorkerID)
	}
}

func (f *feedView) addCompleted(e completedEntry) {
	f.completed = append(f.completed, e)
	// If autoScroll, keep viewport pinned to bottom.
	// The actual clamping happens in view().
}

// scrollDown moves the viewport down one line and disables autoScroll.
func (f *feedView) scrollDown() {
	f.autoScroll = false
	f.scrollOffset++
}

// scrollUp moves the viewport up one line and disables autoScroll.
func (f *feedView) scrollUp() {
	f.autoScroll = false
	if f.scrollOffset > 0 {
		f.scrollOffset--
	}
}

// scrollToTop jumps to the first completed entry.
func (f *feedView) scrollToTop() {
	f.autoScroll = false
	f.scrollOffset = 0
}

// scrollToBottom jumps to the most recent completed entry and re-enables autoScroll.
func (f *feedView) scrollToBottom() {
	f.autoScroll = true
}

func (f *feedView) view(width, height int, speed float64) string {
	if width < 20 {
		width = 20
	}

	// Reserve space: 1 divider per visible section.
	// In-flight: capped at min(len, height/3).
	// Errors: capped at min(len, 5).
	// Completed: fills remaining space.

	maxInFlight := height / 3
	if maxInFlight < 1 {
		maxInFlight = 1
	}
	inFlightCount := len(f.inFlight)
	if inFlightCount > maxInFlight {
		inFlightCount = maxInFlight
	}

	maxErrors := 5
	errCount := len(f.errors)
	if errCount > maxErrors {
		errCount = maxErrors
	}

	// Calculate divider lines needed.
	dividers := 0
	if inFlightCount > 0 {
		dividers++
	}
	if errCount > 0 {
		dividers++
	}
	if len(f.completed) > 0 {
		dividers++
	}

	completedHeight := height - inFlightCount - errCount - dividers
	if completedHeight < 1 {
		completedHeight = 1
	}

	// Clamp scroll offset.
	maxOffset := len(f.completed) - completedHeight
	if maxOffset < 0 {
		maxOffset = 0
	}
	if f.autoScroll {
		f.scrollOffset = maxOffset
	}
	if f.scrollOffset > maxOffset {
		f.scrollOffset = maxOffset
	}
	if f.scrollOffset < 0 {
		f.scrollOffset = 0
	}

	var b strings.Builder

	// Section 1: In-flight.
	inFlightLines := f.renderInFlight(width, maxInFlight)
	if inFlightLines != "" {
		b.WriteString(styleDivider.Render("─ in-flight"))
		b.WriteByte('\n')
		b.WriteString(inFlightLines)
	}

	// Section 2: Completed (scrollable viewport).
	completedLines := f.renderCompletedViewport(width, completedHeight, speed)
	if completedLines != "" {
		label := fmt.Sprintf("─ completed (%d)", len(f.completed))
		b.WriteString(styleDivider.Render(label))
		b.WriteByte('\n')
		b.WriteString(completedLines)
	}

	// Section 3: Errors (pinned at bottom).
	errorLines := f.renderErrors(width, errCount)
	if errorLines != "" {
		label := fmt.Sprintf("─ errors (%d)", len(f.errors))
		b.WriteString(styleDivider.Render(label))
		b.WriteByte('\n')
		b.WriteString(errorLines)
	}

	return b.String()
}

func (f *feedView) renderInFlight(width, maxLines int) string {
	if len(f.inFlight) == 0 {
		return ""
	}

	var b strings.Builder
	count := 0
	for _, e := range f.inFlight {
		if count >= maxLines {
			break
		}
		path := f.styledPath(e.path)
		sizeStr := styleFileSize.Render(ui.FormatBytes(e.size))

		var pct float64
		if e.size > 0 {
			pct = float64(e.progress) / float64(e.size)
		}
		miniBar := renderMiniBar(pct)

		line := fmt.Sprintf("  %s  %s  %s  %s",
			styleInFlight.Render("⟩"),
			path,
			sizeStr,
			miniBar,
		)
		b.WriteString(line)
		b.WriteByte('\n')
		count++
	}
	return b.String()
}

func (f *feedView) renderCompletedViewport(width, viewportHeight int, speed float64) string {
	if len(f.completed) == 0 {
		return ""
	}

	var b strings.Builder
	end := f.scrollOffset + viewportHeight
	if end > len(f.completed) {
		end = len(f.completed)
	}
	start := f.scrollOffset
	if start < 0 {
		start = 0
	}

	for _, e := range f.completed[start:end] {
		var icon, extra string
		path := f.styledPath(e.path)
		sizeStr := styleFileSize.Render(fmt.Sprintf("%10s", ui.FormatBytes(e.size)))

		switch {
		case e.failed:
			icon = styleIconFailed.Render("✗")
			extra = styleError.Render(e.errMsg)
		case e.skipped:
			icon = styleIconSkipped.Render("–")
			extra = styleIconSkipped.Render("skipped")
		default:
			icon = styleIconDone.Render("✓")
			if speed > 0 {
				extra = styleFileSpeed.Render(ui.FormatRate(speed))
			}
		}

		line := fmt.Sprintf("  %s  %s  %s", icon, path, sizeStr)
		if extra != "" {
			line += "  " + extra
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func (f *feedView) renderErrors(width, maxLines int) string {
	if len(f.errors) == 0 {
		return ""
	}

	var b strings.Builder
	// Show the most recent errors (tail).
	start := len(f.errors) - maxLines
	if start < 0 {
		start = 0
	}
	for _, e := range f.errors[start:] {
		path := styleErrorPath.Render(ui.StripRoot(f.dstRoot, e.path))
		errMsg := styleError.Render(e.err)
		line := fmt.Sprintf("  %s  %s  %s", styleIconFailed.Render("✗"), path, errMsg)
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func (f *feedView) styledPath(path string) string {
	path = ui.StripRoot(f.dstRoot, path)
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	if dir == "." || dir == "" {
		return styleFilePath.Render(base)
	}
	return styleFileDir.Render(dir+"/") + styleFilePath.Render(base)
}

// renderMiniBar renders a 2-rune progress indicator.
func renderMiniBar(pct float64) string {
	if pct <= 0 {
		return styleProgressEmpty.Render("□□")
	}
	if pct >= 1 {
		return styleProgressFilled.Render("▪▪")
	}
	if pct < 0.5 {
		return styleProgressFilled.Render("▪") + styleProgressEmpty.Render("□")
	}
	return styleProgressFilled.Render("▪▪")
}
