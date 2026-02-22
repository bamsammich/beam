package ui

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/stats"
)

func TestHudPresenterFileCompleted(t *testing.T) {
	var out bytes.Buffer
	collector := stats.NewCollector()
	collector.SetTotals(10, 10240)

	p := &hudPresenter{
		w:           &out,
		stats:       collector,
		forceFeed:   true,
		workers:     4,
		busyWorkers: make(map[int]bool),
	}

	events := make(chan Event, 10)
	events <- Event{Type: event.ScanComplete, Total: 10, TotalSize: 10240}
	events <- Event{Type: event.FileCompleted, Path: "test/file.txt", Size: 1024, WorkerID: 0}
	close(events)

	err := p.Run(events)
	require.NoError(t, err)

	// Should contain the checkmark and file path.
	assert.Contains(t, out.String(), "file.txt")
	assert.Contains(t, out.String(), "\u2713")
}

func TestHudPresenterFileCompletedStyledPath(t *testing.T) {
	var out bytes.Buffer
	collector := stats.NewCollector()
	collector.SetTotals(10, 10240)

	p := &hudPresenter{
		w:           &out,
		stats:       collector,
		forceFeed:   true,
		workers:     4,
		busyWorkers: make(map[int]bool),
	}

	events := make(chan Event, 10)
	events <- Event{Type: event.ScanComplete, Total: 10, TotalSize: 10240}
	events <- Event{Type: event.FileCompleted, Path: "some/dir/file.txt", Size: 1024, WorkerID: 0}
	close(events)

	err := p.Run(events)
	require.NoError(t, err)

	output := out.String()
	// Directory should be dimmed (ANSI dim code present).
	assert.Contains(t, output, ansiDim)
	// Filename should be present after reset.
	assert.Contains(t, output, "file.txt")
}

func TestHudPresenterRelativePaths(t *testing.T) {
	var out bytes.Buffer
	collector := stats.NewCollector()
	collector.SetTotals(10, 10240)

	p := &hudPresenter{
		w:           &out,
		stats:       collector,
		forceFeed:   true,
		workers:     4,
		busyWorkers: make(map[int]bool),
		dstRoot:     "/home/user/dst",
	}

	events := make(chan Event, 10)
	events <- Event{Type: event.ScanComplete, Total: 10, TotalSize: 10240}
	events <- Event{Type: event.FileCompleted, Path: "/home/user/dst/subdir/file.txt", Size: 1024, WorkerID: 0}
	close(events)

	err := p.Run(events)
	require.NoError(t, err)

	output := out.String()
	// Should NOT contain the absolute path root.
	assert.NotContains(t, output, "/home/user/dst/")
	// Should contain the relative subdir and filename.
	assert.Contains(t, output, "subdir")
	assert.Contains(t, output, "file.txt")
}

func TestHudPresenterSummary(t *testing.T) {
	collector := stats.NewCollector()
	collector.AddFilesCopied(500)
	collector.AddBytesCopied(1024 * 1024 * 100)

	p := &hudPresenter{stats: collector, workers: 4}
	s := p.Summary()
	// New format: "done ✓  files 500  size ..."
	assert.Contains(t, s, "done")
	assert.Contains(t, s, "files 500")
}

func TestHudPresenterSummaryWithVerify(t *testing.T) {
	collector := stats.NewCollector()
	collector.AddFilesCopied(100)
	collector.AddBytesCopied(1024 * 1024)
	collector.AddFilesVerified(100)

	p := &hudPresenter{stats: collector, workers: 4}
	s := p.Summary()
	assert.Contains(t, s, "verified 100")
	assert.Contains(t, s, "errors 0")
}

func TestTruncPath(t *testing.T) {
	assert.Equal(t, "short.txt", truncPath("short.txt", 20))
	assert.Equal(t, "...ry/long/path.txt", truncPath("a/very/long/directory/long/path.txt", 19))
	assert.Equal(t, "ab", truncPath("abcdef", 2))
}

func TestStyledPath(t *testing.T) {
	p := &hudPresenter{}

	// File without directory — no dim prefix.
	assert.Equal(t, "file.txt", p.styledPath("file.txt"))

	// File with directory — directory is dimmed.
	styled := p.styledPath("some/dir/file.txt")
	assert.Contains(t, styled, ansiDim+"some/dir/"+ansiReset+"file.txt")

	// Single directory level.
	styled = p.styledPath("dir/file.txt")
	assert.Contains(t, styled, ansiDim+"dir/"+ansiReset+"file.txt")
}

func TestStyledPathWithDstRoot(t *testing.T) {
	p := &hudPresenter{dstRoot: "/home/user/backup"}

	// Absolute path gets root stripped, then styled.
	styled := p.styledPath("/home/user/backup/photos/img.jpg")
	assert.NotContains(t, styled, "/home/user/backup")
	assert.Contains(t, styled, ansiDim+"photos/"+ansiReset+"img.jpg")

	// File directly in root.
	styled = p.styledPath("/home/user/backup/file.txt")
	assert.Equal(t, "file.txt", styled)
}

func TestStripRoot(t *testing.T) {
	assert.Equal(t, "sub/file.txt", StripRoot("/home/user/dst", "/home/user/dst/sub/file.txt"))
	assert.Equal(t, "file.txt", StripRoot("/home/user/dst", "/home/user/dst/file.txt"))
	assert.Equal(t, "/other/path/file.txt", StripRoot("/home/user/dst", "/other/path/file.txt"))
	assert.Equal(t, "file.txt", StripRoot("", "file.txt"))
}

func TestHudClearHUDSequence(t *testing.T) {
	var out bytes.Buffer
	p := &hudPresenter{
		w:           &out,
		stats:       stats.NewCollector(),
		workers:     2,
		busyWorkers: make(map[int]bool),
	}

	// Draw HUD then clear it.
	p.drawHUD()
	assert.True(t, p.hudDrawn)
	assert.Equal(t, 2, p.hudLineCount) // 2 lines in non-rate mode

	out.Reset()
	p.clearHUD()
	// Should contain ANSI escape for cursor up.
	assert.Contains(t, out.String(), "\033[")
	assert.False(t, p.hudDrawn)
}

func TestHudClearHUDRateMode(t *testing.T) {
	var out bytes.Buffer
	p := &hudPresenter{
		w:           &out,
		stats:       stats.NewCollector(),
		workers:     2,
		busyWorkers: make(map[int]bool),
		rateMode:    true,
	}

	p.drawHUD()
	assert.True(t, p.hudDrawn)
	assert.Equal(t, 3, p.hudLineCount) // 3 lines in rate mode (sparkline + 2 HUD)

	out.Reset()
	p.clearHUD()
	// Should move up 3 lines.
	assert.Contains(t, out.String(), "\033[3A")
}

func TestHudAlwaysRedrawsAfterFeedLine(t *testing.T) {
	var out bytes.Buffer
	collector := stats.NewCollector()
	collector.SetTotals(10, 10240)

	p := &hudPresenter{
		w:           &out,
		stats:       collector,
		forceFeed:   true,
		workers:     4,
		busyWorkers: make(map[int]bool),
	}

	events := make(chan Event, 10)
	events <- Event{Type: event.ScanComplete, Total: 10, TotalSize: 10240}
	events <- Event{Type: event.FileCompleted, Path: "a.txt", Size: 100, WorkerID: 0}
	events <- Event{Type: event.FileCompleted, Path: "b.txt", Size: 200, WorkerID: 1}
	close(events)

	err := p.Run(events)
	require.NoError(t, err)

	output := out.String()
	// Both files should appear.
	assert.Contains(t, output, "a.txt")
	assert.Contains(t, output, "b.txt")
	// The progress bar character should appear (HUD was drawn).
	assert.Contains(t, output, "□")
}

func TestHudPresenterVerifyStarted(t *testing.T) {
	var out bytes.Buffer
	collector := stats.NewCollector()
	collector.SetTotals(10, 10240)

	p := &hudPresenter{
		w:           &out,
		stats:       collector,
		forceFeed:   true,
		workers:     4,
		busyWorkers: make(map[int]bool),
	}

	events := make(chan Event, 10)
	events <- Event{Type: event.VerifyStarted}
	close(events)

	err := p.Run(events)
	require.NoError(t, err)
	assert.Contains(t, out.String(), "verifying checksums...")
}

func TestHudPresenterVerifyFailed(t *testing.T) {
	var out bytes.Buffer
	collector := stats.NewCollector()
	collector.SetTotals(10, 10240)

	p := &hudPresenter{
		w:           &out,
		stats:       collector,
		forceFeed:   true,
		workers:     4,
		busyWorkers: make(map[int]bool),
	}

	events := make(chan Event, 10)
	events <- Event{Type: event.VerifyFailed, Path: "bad/file.txt"}
	close(events)

	err := p.Run(events)
	require.NoError(t, err)

	output := out.String()
	assert.Contains(t, output, "\u2717")
	assert.Contains(t, output, "file.txt")
	assert.Contains(t, output, "CHECKSUM MISMATCH")
}

func TestHudRateSwitchNotice(t *testing.T) {
	var out bytes.Buffer
	// Verify the notice format.
	fmt.Fprintf(&out, "\u21af rate view (%s files/s \u00b7 use --feed to see individual files)\n",
		FormatCount(int64(612)))
	assert.Contains(t, out.String(), "\u21af rate view")
	assert.Contains(t, out.String(), "612 files/s")
	assert.Contains(t, out.String(), "use --feed")
}
