package ui

import "github.com/bamsammich/beam/internal/stats"

// quietPresenter consumes events but produces no output.
type quietPresenter struct {
	stats stats.Reader
}

func (p *quietPresenter) Run(events <-chan Event) error {
	for ev := range events {
		p.handleEvent(ev)
	}
	return nil
}

func (p *quietPresenter) handleEvent(_ Event) {
	// Totals are set on the collector directly by the engine;
	// presenters only read from the collector, never write.
}

func (p *quietPresenter) Summary() string {
	return ""
}
