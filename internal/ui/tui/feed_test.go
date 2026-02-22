package tui

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/event"
)

func TestFeedView_HandleFileStarted(t *testing.T) {
	f := newFeedView("/dst")
	f.handleEvent(event.Event{
		Type:      event.FileStarted,
		Path:      "/dst/a.txt",
		Size:      1024,
		WorkerID:  1,
		Timestamp: time.Now(),
	})

	require.Len(t, f.inFlight, 1)
	assert.Equal(t, "/dst/a.txt", f.inFlight[1].path)
	assert.Equal(t, int64(1024), f.inFlight[1].size)
}

func TestFeedView_HandleFileCompleted(t *testing.T) {
	f := newFeedView("/dst")
	f.handleEvent(event.Event{
		Type:     event.FileStarted,
		Path:     "/dst/a.txt",
		Size:     1024,
		WorkerID: 1,
	})
	f.handleEvent(event.Event{
		Type:     event.FileCompleted,
		Path:     "/dst/a.txt",
		Size:     1024,
		WorkerID: 1,
	})

	assert.Empty(t, f.inFlight)
	require.Len(t, f.completed, 1)
	assert.Equal(t, "/dst/a.txt", f.completed[0].path)
	assert.False(t, f.completed[0].failed)
	assert.False(t, f.completed[0].skipped)
}

func TestFeedView_HandleFileFailed(t *testing.T) {
	f := newFeedView("/dst")
	f.handleEvent(event.Event{
		Type:     event.FileFailed,
		Path:     "/dst/b.txt",
		Size:     2048,
		WorkerID: 2,
		Error:    errors.New("permission denied"),
	})

	require.Len(t, f.completed, 1)
	assert.True(t, f.completed[0].failed)
	assert.Equal(t, "permission denied", f.completed[0].errMsg)
	require.Len(t, f.errors, 1)
	assert.Equal(t, "permission denied", f.errors[0].err)
}

func TestFeedView_HandleFileSkipped(t *testing.T) {
	f := newFeedView("/dst")
	f.handleEvent(event.Event{
		Type: event.FileSkipped,
		Path: "/dst/c.txt",
		Size: 512,
	})

	require.Len(t, f.completed, 1)
	assert.True(t, f.completed[0].skipped)
}

func TestFeedView_UnboundedCompleted(t *testing.T) {
	f := newFeedView("/dst")

	// Add many entries — they should all be kept (no ring buffer).
	for i := range 100 {
		f.handleEvent(event.Event{
			Type:     event.FileCompleted,
			Path:     "/dst/" + string(rune('a'+i%26)) + ".txt",
			Size:     100,
			WorkerID: 0,
		})
	}

	assert.Len(t, f.completed, 100)
}

func TestFeedView_ScrollDown(t *testing.T) {
	f := newFeedView("/dst")
	assert.True(t, f.autoScroll)

	f.scrollDown()
	assert.False(t, f.autoScroll)
	assert.Equal(t, 1, f.scrollOffset)
}

func TestFeedView_ScrollUp(t *testing.T) {
	f := newFeedView("/dst")
	f.scrollOffset = 5
	f.autoScroll = false

	f.scrollUp()
	assert.Equal(t, 4, f.scrollOffset)

	// Should not go below 0.
	f.scrollOffset = 0
	f.scrollUp()
	assert.Equal(t, 0, f.scrollOffset)
}

func TestFeedView_ScrollToTop(t *testing.T) {
	f := newFeedView("/dst")
	f.scrollOffset = 10

	f.scrollToTop()
	assert.Equal(t, 0, f.scrollOffset)
	assert.False(t, f.autoScroll)
}

func TestFeedView_ScrollToBottom(t *testing.T) {
	f := newFeedView("/dst")
	f.autoScroll = false

	f.scrollToBottom()
	assert.True(t, f.autoScroll)
}

func TestFeedView_ViewRendersNonEmpty(t *testing.T) {
	f := newFeedView("/dst")
	f.handleEvent(event.Event{
		Type:     event.FileStarted,
		Path:     "/dst/in-progress.txt",
		Size:     4096,
		WorkerID: 0,
	})
	f.handleEvent(event.Event{
		Type:     event.FileCompleted,
		Path:     "/dst/done.txt",
		Size:     1024,
		WorkerID: 1,
	})
	f.handleEvent(event.Event{
		Type:     event.FileFailed,
		Path:     "/dst/fail.txt",
		Size:     512,
		WorkerID: 2,
		Error:    errors.New("read error"),
	})

	out := f.view(80, 40, 1024*1024)
	assert.Contains(t, out, "in-flight")
	assert.Contains(t, out, "completed")
	assert.Contains(t, out, "errors")
	assert.Contains(t, out, "in-progress.txt")
	assert.Contains(t, out, "done.txt")
	assert.Contains(t, out, "fail.txt")
	assert.Contains(t, out, "read error")
}

func TestFeedView_ViewScrollClamping(t *testing.T) {
	f := newFeedView("/dst")
	for i := range 5 {
		f.handleEvent(event.Event{
			Type:     event.FileCompleted,
			Path:     "/dst/" + string(rune('a'+i)) + ".txt",
			Size:     100,
			WorkerID: 0,
		})
	}

	// Set scroll offset beyond max — should be clamped by view().
	f.autoScroll = false
	f.scrollOffset = 999

	out := f.view(80, 20, 0)
	assert.NotEmpty(t, out)
	// After clamping, offset should be valid.
	assert.LessOrEqual(t, f.scrollOffset, len(f.completed))
}

func TestFeedView_VerifyFailed(t *testing.T) {
	f := newFeedView("/dst")
	f.handleEvent(event.Event{
		Type:      event.VerifyFailed,
		Path:      "/dst/bad.dat",
		Timestamp: time.Now(),
	})

	require.Len(t, f.errors, 1)
	assert.Equal(t, "CHECKSUM MISMATCH", f.errors[0].err)
}

func TestMiniBar(t *testing.T) {
	assert.Contains(t, renderMiniBar(0), "□□")
	assert.Contains(t, renderMiniBar(0.3), "▪")
	assert.Contains(t, renderMiniBar(1.0), "▪▪")
}
