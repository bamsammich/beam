package ui

import (
	"bytes"
	"strings"
	"testing"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/stretchr/testify/assert"
)

func TestPlainPresenterFileCompleted(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	collector := stats.NewCollector()

	p := &plainPresenter{w: &out, errW: &errOut, stats: collector}

	events := make(chan Event, 10)
	events <- Event{Type: event.FileCompleted, Path: "dir/file.txt", Size: 1024}
	events <- Event{Type: event.FileCompleted, Path: "dir/big.bin", Size: 1024 * 1024 * 100}
	close(events)

	err := p.Run(events)
	assert.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	assert.Len(t, lines, 2)
	assert.Contains(t, lines[0], "dir/file.txt")
	assert.Contains(t, lines[1], "dir/big.bin")
}

func TestPlainPresenterFileFailed(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	collector := stats.NewCollector()

	p := &plainPresenter{w: &out, errW: &errOut, stats: collector}

	events := make(chan Event, 5)
	events <- Event{Type: event.FileFailed, Path: "fail.txt", Size: 512, Error: assert.AnError}
	close(events)

	err := p.Run(events)
	assert.NoError(t, err)

	assert.Contains(t, out.String(), "fail.txt")
	assert.Contains(t, out.String(), assert.AnError.Error())
}

func TestPlainPresenterFileSkipped(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	collector := stats.NewCollector()

	p := &plainPresenter{w: &out, errW: &errOut, stats: collector}

	events := make(chan Event, 5)
	events <- Event{Type: event.FileSkipped, Path: "skip.txt"}
	close(events)

	err := p.Run(events)
	assert.NoError(t, err)

	assert.Contains(t, out.String(), "skip.txt")
	assert.Contains(t, out.String(), "skipped")
}

func TestPlainPresenterDeleteFile(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	collector := stats.NewCollector()

	p := &plainPresenter{w: &out, errW: &errOut, stats: collector}

	events := make(chan Event, 5)
	events <- Event{Type: event.DeleteFile, Path: "extra.txt"}
	close(events)

	err := p.Run(events)
	assert.NoError(t, err)

	assert.Contains(t, out.String(), "delete: extra.txt")
}

func TestPlainPresenterVerifyStarted(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	collector := stats.NewCollector()

	p := &plainPresenter{w: &out, errW: &errOut, stats: collector}

	events := make(chan Event, 5)
	events <- Event{Type: event.VerifyStarted}
	close(events)

	err := p.Run(events)
	assert.NoError(t, err)
	assert.Contains(t, out.String(), "verifying...")
}

func TestPlainPresenterVerifyFailed(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	collector := stats.NewCollector()

	p := &plainPresenter{w: &out, errW: &errOut, stats: collector}

	events := make(chan Event, 5)
	events <- Event{Type: event.VerifyFailed, Path: "bad/file.txt"}
	close(events)

	err := p.Run(events)
	assert.NoError(t, err)
	assert.Contains(t, out.String(), "MISMATCH: bad/file.txt")
}

func TestPlainPresenterSummary(t *testing.T) {
	collector := stats.NewCollector()
	collector.AddFilesCopied(100)
	collector.AddBytesCopied(1024 * 1024)

	p := &plainPresenter{stats: collector}
	s := p.Summary()
	assert.Contains(t, s, "files 100")
	assert.Contains(t, s, "errors 0")
}
