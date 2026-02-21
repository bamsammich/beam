package tui

import (
	"testing"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/stretchr/testify/assert"
)

func TestRateView_WorkerTracking(t *testing.T) {
	r := newRateView()

	r.handleEvent(event.Event{Type: event.FileStarted, WorkerID: 0})
	r.handleEvent(event.Event{Type: event.FileStarted, WorkerID: 1})
	assert.Len(t, r.busyWorkers, 2)

	r.handleEvent(event.Event{Type: event.FileCompleted, WorkerID: 0})
	assert.Len(t, r.busyWorkers, 1)
	assert.True(t, r.busyWorkers[1])
}

func TestRateView_ViewRendersNonEmpty(t *testing.T) {
	r := newRateView()
	r.handleEvent(event.Event{Type: event.FileStarted, WorkerID: 0})

	c := stats.NewCollector()
	c.SetTotals(100, 1024*1024*1024)
	c.AddFilesCopied(10)
	c.AddBytesCopied(100 * 1024 * 1024)
	c.Tick()

	snap := c.Snapshot()
	out := r.view(80, 40, snap, c, 4)

	assert.NotEmpty(t, out)
	assert.Contains(t, out, "workers")
	assert.Contains(t, out, "files/s")
}

func TestRateView_WorkerGrid(t *testing.T) {
	r := newRateView()
	r.busyWorkers[0] = true
	r.busyWorkers[2] = true

	grid := r.renderWorkerGrid(4)
	assert.NotEmpty(t, grid)
	// Should contain both busy and idle indicators.
	assert.Contains(t, grid, "▪")
	assert.Contains(t, grid, "□")
}
