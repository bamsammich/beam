package tui

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/ui"
)

type viewMode int

const (
	viewFeed viewMode = iota
	viewRate
)

// Bubble Tea messages.
type engineEventMsg event.Event
type channelDoneMsg struct{}
type tickMsg time.Time
type saveResultMsg struct{ err error }

// readNextEvent returns a tea.Cmd that blocks on the event channel.
func readNextEvent(ch <-chan event.Event) tea.Cmd {
	return func() tea.Msg {
		ev, ok := <-ch
		if !ok {
			return channelDoneMsg{}
		}
		return engineEventMsg(ev)
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// saveModal manages the text input overlay for saving results.
type saveModal struct {
	active bool
	input  string
	cursor int
}

func (s *saveModal) insertRune(r rune) {
	s.input = s.input[:s.cursor] + string(r) + s.input[s.cursor:]
	s.cursor++
}

func (s *saveModal) backspace() {
	if s.cursor > 0 {
		s.input = s.input[:s.cursor-1] + s.input[s.cursor:]
		s.cursor--
	}
}

func (s *saveModal) deleteChar() {
	if s.cursor < len(s.input) {
		s.input = s.input[:s.cursor] + s.input[s.cursor+1:]
	}
}

func (s *saveModal) moveLeft() {
	if s.cursor > 0 {
		s.cursor--
	}
}

func (s *saveModal) moveRight() {
	if s.cursor < len(s.input) {
		s.cursor++
	}
}

func (s *saveModal) render() string {
	prompt := styleSavePrompt.Render("Save to: ")
	before := s.input[:s.cursor]
	after := s.input[s.cursor:]
	cursor := styleSaveInput.Render("█")
	return "  " + prompt + styleSaveInput.Render(before) + cursor + styleSaveInput.Render(after)
}

// Model is the root Bubble Tea model.
type Model struct {
	events  <-chan event.Event
	stats   stats.ReadTicker
	workers int
	dstRoot string
	srcRoot string

	mode      viewMode
	feed      feedView
	rate      rateView
	width     int
	height    int
	statusMsg string // transient notification
	done      bool   // transfer complete
	quitting  bool

	lastSnap  stats.Snapshot
	lastSpeed float64
	lastETA   time.Duration

	// Worker throttle.
	workerLimit *atomic.Int32 // nil = no throttling
	maxWorkers  int

	// Save modal.
	save saveModal
}

// NewModel creates a new TUI model.
func NewModel(events <-chan event.Event, collector stats.ReadTicker, workers int, dstRoot, srcRoot string, workerLimit *atomic.Int32) Model {
	return Model{
		events:      events,
		stats:       collector,
		workers:     workers,
		dstRoot:     dstRoot,
		srcRoot:     srcRoot,
		feed:        newFeedView(dstRoot),
		rate:        newRateView(),
		width:       80,
		height:      24,
		workerLimit: workerLimit,
		maxWorkers:  workers,
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(
		readNextEvent(m.events),
		tickCmd(),
	)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKey(msg)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case engineEventMsg:
		return m.handleEngineEvent(event.Event(msg))

	case channelDoneMsg:
		m.done = true
		m.lastSnap = m.stats.Snapshot()
		m.lastSpeed = m.stats.RollingSpeed(10)
		m.lastETA = 0
		return m, tickCmd()

	case tickMsg:
		m.stats.Tick()
		m.lastSnap = m.stats.Snapshot()
		m.lastSpeed = m.stats.RollingSpeed(10)
		m.lastETA = m.stats.ETA()
		return m, tickCmd()

	case saveResultMsg:
		if msg.err != nil {
			m.statusMsg = fmt.Sprintf("save failed: %v", msg.err)
		} else {
			m.statusMsg = fmt.Sprintf("saved to %s", m.save.input)
		}
		m.save.active = false
		return m, nil
	}

	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// When save modal is active, capture all input.
	if m.save.active {
		return m.handleSaveKey(msg)
	}

	switch msg.String() {
	case "q", "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	case "r":
		m.mode = viewRate
		m.statusMsg = ""
		return m, nil

	case "f":
		m.mode = viewFeed
		m.statusMsg = ""
		return m, nil

	case "e":
		m.mode = viewFeed
		m.statusMsg = ""
		return m, nil

	case "p":
		m.statusMsg = "pause: not yet implemented"
		return m, nil

	case "+", "=":
		return m.adjustWorkers(1)

	case "-":
		return m.adjustWorkers(-1)

	case "v":
		m.statusMsg = "verify toggle: not yet implemented"
		return m, nil

	// Scroll keys for feed view.
	case "j", "down":
		if m.mode == viewFeed {
			m.feed.scrollDown()
		}
		return m, nil

	case "k", "up":
		if m.mode == viewFeed {
			m.feed.scrollUp()
		}
		return m, nil

	case "G":
		if m.mode == viewFeed {
			m.feed.scrollToBottom()
		}
		return m, nil

	case "g":
		if m.mode == viewFeed {
			m.feed.scrollToTop()
		}
		return m, nil

	case "s":
		if m.done {
			m.save.active = true
			m.save.input = fmt.Sprintf("beam-%s.log", time.Now().Format("2006-01-02-150405"))
			m.save.cursor = len(m.save.input)
			m.statusMsg = ""
		}
		return m, nil
	}

	return m, nil
}

func (m Model) handleSaveKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEscape:
		m.save.active = false
		m.statusMsg = ""
		return m, nil

	case tea.KeyEnter:
		path := m.save.input
		return m, m.writeReport(path)

	case tea.KeyBackspace:
		m.save.backspace()
		return m, nil

	case tea.KeyDelete:
		m.save.deleteChar()
		return m, nil

	case tea.KeyLeft:
		m.save.moveLeft()
		return m, nil

	case tea.KeyRight:
		m.save.moveRight()
		return m, nil

	case tea.KeyRunes:
		for _, r := range msg.Runes {
			m.save.insertRune(r)
		}
		return m, nil
	}

	return m, nil
}

func (m Model) adjustWorkers(delta int) (tea.Model, tea.Cmd) {
	if m.workerLimit == nil {
		m.statusMsg = "worker adjustment: not available"
		return m, nil
	}

	current := m.workerLimit.Load()
	next := current + int32(delta)
	if next < 1 {
		next = 1
	}
	if next > int32(m.maxWorkers) {
		next = int32(m.maxWorkers)
	}
	m.workerLimit.Store(next)
	m.statusMsg = fmt.Sprintf("workers: %d / %d", next, m.maxWorkers)
	return m, nil
}

func (m Model) activeWorkerCount() int {
	if m.workerLimit != nil {
		return int(m.workerLimit.Load())
	}
	return m.workers
}

func (m Model) writeReport(path string) tea.Cmd {
	// Capture data needed by the goroutine.
	snap := m.lastSnap
	srcRoot := m.srcRoot
	dstRoot := m.dstRoot
	completed := make([]completedEntry, len(m.feed.completed))
	copy(completed, m.feed.completed)

	return func() tea.Msg {
		var b strings.Builder

		b.WriteString("beam transfer report\n")
		b.WriteString("====================\n")
		fmt.Fprintf(&b, "source:      %s\n", srcRoot)
		fmt.Fprintf(&b, "destination: %s\n", dstRoot)
		fmt.Fprintf(&b, "completed:   %s\n", time.Now().Format("2006-01-02 15:04:05"))
		fmt.Fprintf(&b, "duration:    %s\n", ui.FormatDuration(snap.Elapsed))
		fmt.Fprintf(&b, "files:       %s\n", ui.FormatCount(snap.FilesCopied))
		fmt.Fprintf(&b, "size:        %s\n", ui.FormatBytes(snap.BytesCopied))
		avgSpeed := 0.0
		if snap.Elapsed.Seconds() > 0 {
			avgSpeed = float64(snap.BytesCopied) / snap.Elapsed.Seconds()
		}
		fmt.Fprintf(&b, "avg speed:   %s\n", ui.FormatRate(avgSpeed))
		fmt.Fprintf(&b, "errors:      %d\n", snap.FilesFailed+snap.FilesVerifyFailed)
		b.WriteString("\n--- files ---\n")

		for _, e := range completed {
			relPath := ui.StripRoot(dstRoot, e.path)
			switch {
			case e.failed:
				fmt.Fprintf(&b, "x  %-50s  %s\n", relPath, e.errMsg)
			case e.skipped:
				fmt.Fprintf(&b, "-  %-50s  skipped\n", relPath)
			default:
				fmt.Fprintf(&b, "v  %-50s  %s\n", relPath, ui.FormatBytes(e.size))
			}
		}

		err := os.WriteFile(path, []byte(b.String()), 0o644) //nolint:gosec // user-chosen path for report output
		return saveResultMsg{err: err}
	}
}

func (m Model) handleEngineEvent(ev event.Event) (tea.Model, tea.Cmd) {
	// Totals are set on the collector directly by the engine;
	// presenters only read from the collector, never write.

	// Forward to both views so they stay in sync.
	m.feed.handleEvent(ev)
	m.rate.handleEvent(ev)

	return m, readNextEvent(m.events)
}

func (m Model) View() string {
	if m.quitting {
		return ""
	}

	var b strings.Builder

	// Header (1 line).
	b.WriteString(m.renderHeader())
	b.WriteByte('\n')

	// Content area.
	contentHeight := m.height - 3 // header (1) + footer (1) + save/status (1)
	if contentHeight < 3 {
		contentHeight = 3
	}

	switch m.mode {
	case viewFeed:
		b.WriteString(m.feed.view(m.width, contentHeight, m.lastSpeed))
	case viewRate:
		b.WriteString(m.rate.view(m.width, contentHeight, m.lastSnap, m.stats, m.activeWorkerCount()))
	}

	// Save modal or status message.
	if m.save.active {
		b.WriteString(m.save.render())
		b.WriteByte('\n')
	} else if m.statusMsg != "" {
		b.WriteString(styleStatus.Render("  " + m.statusMsg))
		b.WriteByte('\n')
	} else {
		b.WriteByte('\n')
	}

	// Footer.
	b.WriteString(m.renderFooter())

	return b.String()
}

func (m Model) renderHeader() string {
	snap := m.lastSnap

	var pct float64
	if snap.BytesTotal > 0 {
		pct = float64(snap.BytesCopied) / float64(snap.BytesTotal)
	}

	bar := ui.ProgressBar(pct, 10)

	activeWorkers := m.activeWorkerCount()

	header := fmt.Sprintf("  %s  %3.0f%%  %s  %s / %s  %s / %s files  eta %s  %dw",
		styleHeaderLabel.Render("beam"),
		pct*100,
		styleProgressFilled.Render(bar),
		ui.FormatBytes(snap.BytesCopied),
		ui.FormatBytes(snap.BytesTotal),
		ui.FormatCount(snap.FilesCopied),
		ui.FormatCount(snap.FilesTotal),
		ui.FormatETA(m.lastETA),
		activeWorkers,
	)

	if m.done {
		header = fmt.Sprintf("  %s  %s  %s  %s / %s files  %s",
			styleHeaderLabel.Render("beam"),
			styleIconDone.Render("done"),
			ui.FormatBytes(snap.BytesCopied),
			ui.FormatCount(snap.FilesCopied),
			ui.FormatCount(snap.FilesTotal),
			ui.FormatDuration(snap.Elapsed),
		)
	}

	return styleHeader.Render(header)
}

func (m Model) renderFooter() string {
	type keybind struct {
		key   string
		label string
	}

	var binds []keybind
	if m.done {
		binds = []keybind{
			{"s", "save"},
			{"j/k", "scroll"},
			{"r", "rate"},
			{"f", "feed"},
			{"q", "quit"},
		}
	} else {
		binds = []keybind{
			{"q", "quit"},
			{"r", "rate"},
			{"f", "feed"},
			{"e", "errors"},
			{"j/k", "scroll"},
			{"+/-", "workers"},
		}
	}

	var parts []string
	for _, kb := range binds {
		parts = append(parts,
			styleKeybindKey.Render(kb.key)+" "+styleKeybindLabel.Render(kb.label))
	}

	return "  " + strings.Join(parts, "   ")
}
