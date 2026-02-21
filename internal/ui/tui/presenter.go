package tui

import (
	"sync/atomic"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/bamsammich/beam/internal/config"
	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/ui"
)

// Config configures the TUI presenter.
type Config struct {
	Stats       *stats.Collector
	Workers     int
	DstRoot     string
	SrcRoot     string
	Theme       config.ThemeConfig
	WorkerLimit *atomic.Int32
}

// Presenter wraps a Bubble Tea program and implements ui.Presenter.
type Presenter struct {
	cfg   Config
	model Model
}

// NewPresenter creates a new TUI presenter.
func NewPresenter(cfg Config) *Presenter {
	ApplyTheme(cfg.Theme)
	return &Presenter{cfg: cfg}
}

// Run starts the Bubble Tea program and blocks until done.
func (p *Presenter) Run(events <-chan event.Event) error {
	p.model = NewModel(events, p.cfg.Stats, p.cfg.Workers, p.cfg.DstRoot, p.cfg.SrcRoot, p.cfg.WorkerLimit)
	prog := tea.NewProgram(
		p.model,
		tea.WithAltScreen(),
		tea.WithoutSignalHandler(),
	)
	finalModel, err := prog.Run()
	if err != nil {
		return err
	}
	p.model = finalModel.(Model)
	return nil
}

// Summary returns the final completion summary line.
func (p *Presenter) Summary() string {
	return ui.CompletionSummary(p.cfg.Stats.Snapshot())
}
