package tui

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/stats"
)

func newTestModel() (Model, *stats.Collector) {
	ch := make(chan event.Event, 10)
	c := stats.NewCollector()
	c.SetTotals(100, 1024*1024*1024)
	return NewModel(ch, c, 8, "/dst", "/src", nil), c
}

func newTestModelWithThrottle() Model {
	ch := make(chan event.Event, 10)
	c := stats.NewCollector()
	c.SetTotals(100, 1024*1024*1024)
	limit := &atomic.Int32{}
	limit.Store(8)
	return NewModel(ch, c, 8, "/dst", "/src", limit)
}

func TestModel_Init(t *testing.T) {
	m, _ := newTestModel()
	cmd := m.Init()
	assert.NotNil(t, cmd)
}

func TestModel_KeyQ_Quits(t *testing.T) {
	m, _ := newTestModel()
	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.True(t, model.quitting)
	assert.NotNil(t, cmd) // tea.Quit
}

func TestModel_KeyR_SwitchesToRate(t *testing.T) {
	m, _ := newTestModel()
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Equal(t, viewRate, model.mode)
}

func TestModel_KeyF_SwitchesToFeed(t *testing.T) {
	m, _ := newTestModel()
	m.mode = viewRate
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Equal(t, viewFeed, model.mode)
}

func TestModel_KeyE_SwitchesToFeed(t *testing.T) {
	m, _ := newTestModel()
	m.mode = viewRate
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Equal(t, viewFeed, model.mode)
}

func TestModel_KeyP_ShowsNotImplemented(t *testing.T) {
	m, _ := newTestModel()
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Contains(t, model.statusMsg, "not yet implemented")
}

func TestModel_WindowResize(t *testing.T) {
	m, _ := newTestModel()
	updated, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Equal(t, 120, model.width)
	assert.Equal(t, 40, model.height)
}

func TestModel_EngineEvent(t *testing.T) {
	m, _ := newTestModel()
	ev := engineEventMsg(event.Event{
		Type:     event.FileStarted,
		Path:     "/dst/test.txt",
		Size:     4096,
		WorkerID: 0,
	})
	updated, cmd := m.Update(ev)
	model, ok := updated.(Model)
	require.True(t, ok)

	require.Len(t, model.feed.inFlight, 1)
	assert.True(t, model.rate.busyWorkers[0])
	assert.NotNil(t, cmd)
}

func TestModel_ChannelDone_StaysOpen(t *testing.T) {
	m, _ := newTestModel()
	updated, cmd := m.Update(channelDoneMsg{})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.True(t, model.done)
	assert.False(t, model.quitting)
	assert.NotNil(t, cmd) // tickCmd keeps TUI alive
}

func TestModel_Tick(t *testing.T) {
	m, c := newTestModel()
	c.AddFilesCopied(5)
	c.AddBytesCopied(1024 * 1024)

	updated, cmd := m.Update(tickMsg(time.Now()))
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Equal(t, int64(5), model.lastSnap.FilesCopied)
	assert.NotNil(t, cmd)
}

func TestModel_ViewFeed(t *testing.T) {
	m, _ := newTestModel()
	m.width = 80
	m.height = 30
	out := m.View()
	assert.Contains(t, out, "beam")
	assert.Contains(t, out, "quit")
}

func TestModel_ViewRate(t *testing.T) {
	m, _ := newTestModel()
	m.mode = viewRate
	m.width = 80
	m.height = 30
	m.stats.Tick()
	m.lastSnap = m.stats.Snapshot()

	out := m.View()
	assert.Contains(t, out, "beam")
	assert.Contains(t, out, "workers")
}

func TestModel_ViewQuitting(t *testing.T) {
	m, _ := newTestModel()
	m.quitting = true
	out := m.View()
	assert.Empty(t, out)
}

func TestModel_ScrollKeys(t *testing.T) {
	m, _ := newTestModel()
	// Add some completed entries.
	for i := range 10 {
		m.feed.handleEvent(event.Event{
			Type:     event.FileCompleted,
			Path:     "/dst/" + string(rune('a'+i)) + ".txt",
			Size:     100,
			WorkerID: 0,
		})
	}

	// j scrolls down.
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.False(t, model.feed.autoScroll)

	// G re-enables autoScroll.
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'G'}})
	model, ok = updated.(Model)
	require.True(t, ok)
	assert.True(t, model.feed.autoScroll)

	// g goes to top.
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'g'}})
	model, ok = updated.(Model)
	require.True(t, ok)
	assert.Equal(t, 0, model.feed.scrollOffset)
	assert.False(t, model.feed.autoScroll)
}

func TestModel_SaveModal_ActivatesOnlyWhenDone(t *testing.T) {
	m, _ := newTestModel()
	m.done = false

	// s should do nothing when not done.
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.False(t, model.save.active)

	// s should activate when done.
	model.done = true
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	model, ok = updated.(Model)
	require.True(t, ok)
	assert.True(t, model.save.active)
	assert.Contains(t, model.save.input, "beam-")
	assert.Contains(t, model.save.input, ".log")
}

func TestModel_SaveModal_EscCancels(t *testing.T) {
	m, _ := newTestModel()
	m.done = true
	m.save.active = true
	m.save.input = "test.log"

	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyEscape})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.False(t, model.save.active)
}

func TestModel_SaveModal_TextInput(t *testing.T) {
	m, _ := newTestModel()
	m.save.active = true
	m.save.input = ""
	m.save.cursor = 0

	// Type "abc".
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'a'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'b'}})
	model, ok = updated.(Model)
	require.True(t, ok)
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}})
	model, ok = updated.(Model)
	require.True(t, ok)

	assert.Equal(t, "abc", model.save.input)
	assert.Equal(t, 3, model.save.cursor)

	// Backspace.
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	model, ok = updated.(Model)
	require.True(t, ok)
	assert.Equal(t, "ab", model.save.input)
}

func TestModel_SaveModal_WritesFile(t *testing.T) {
	m, _ := newTestModel()
	m.done = true
	m.srcRoot = "/src"
	m.dstRoot = "/dst"
	m.lastSnap = m.stats.Snapshot()

	// Add a completed entry.
	m.feed.handleEvent(event.Event{
		Type:     event.FileCompleted,
		Path:     "/dst/test.txt",
		Size:     1024,
		WorkerID: 0,
	})

	path := filepath.Join(t.TempDir(), "test-report.log")
	m.save.input = path

	cmd := m.writeReport(path)
	msg := cmd()
	result, ok := msg.(saveResultMsg)
	require.True(t, ok)
	require.NoError(t, result.err)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(content), "beam transfer report")
	assert.Contains(t, string(content), "/src")
	assert.Contains(t, string(content), "/dst")
	assert.Contains(t, string(content), "test.txt")
}

func TestModel_FooterChangesWhenDone(t *testing.T) {
	m, _ := newTestModel()
	m.done = false
	footer := m.renderFooter()
	assert.Contains(t, footer, "workers")

	m.done = true
	footer = m.renderFooter()
	assert.Contains(t, footer, "save")
	assert.Contains(t, footer, "scroll")
}

func TestModel_WorkerAdjust_WithThrottle(t *testing.T) {
	m := newTestModelWithThrottle()

	// Increment.
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'+'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Contains(t, model.statusMsg, "workers")
	// Already at max (8), so should stay at 8.
	assert.Equal(t, int32(8), model.workerLimit.Load())

	// Decrement.
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'-'}})
	model, ok = updated.(Model)
	require.True(t, ok)
	assert.Equal(t, int32(7), model.workerLimit.Load())
}

func TestModel_WorkerAdjust_WithoutThrottle(t *testing.T) {
	m, _ := newTestModel() // no throttle
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'+'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Contains(t, model.statusMsg, "not available")
}

func TestModel_WorkerAdjust_ClampsToMin(t *testing.T) {
	m := newTestModelWithThrottle()
	m.workerLimit.Store(1)

	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'-'}})
	model, ok := updated.(Model)
	require.True(t, ok)
	assert.Equal(t, int32(1), model.workerLimit.Load())
}
