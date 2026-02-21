package ui

import (
	"io"

	"github.com/bamsammich/beam/internal/stats"
)

// Presenter consumes events and displays progress.
type Presenter interface {
	// Run consumes events until the channel closes. Blocks until done.
	Run(events <-chan Event) error
	// Summary returns the final summary line.
	Summary() string
}

// Config configures a Presenter.
type Config struct {
	Writer     io.Writer
	ErrWriter  io.Writer // stderr for progress in non-TTY mode
	IsTTY      bool
	Quiet      bool
	Verbose    bool
	ForceFeed  bool
	ForceRate  bool
	NoProgress bool
	Stats      stats.ReadTicker
	Workers    int
	DstRoot    string // destination root path, stripped from displayed paths
}

// NewPresenter creates the appropriate presenter based on configuration.
func NewPresenter(cfg Config) Presenter {
	if cfg.Quiet {
		return &quietPresenter{stats: cfg.Stats}
	}
	if !cfg.IsTTY || cfg.NoProgress {
		return &plainPresenter{
			w:       cfg.Writer,
			errW:    cfg.ErrWriter,
			stats:   cfg.Stats,
			dstRoot: cfg.DstRoot,
		}
	}
	return &hudPresenter{
		w:         cfg.ErrWriter, // HUD renders to stderr (the TTY)
		stats:     cfg.Stats,
		forceFeed: cfg.ForceFeed,
		forceRate: cfg.ForceRate,
		workers:   cfg.Workers,
		dstRoot:   cfg.DstRoot,
	}
}
