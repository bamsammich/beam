package ui

import "github.com/bamsammich/beam/internal/stats"

// quietPresenter consumes events but produces no output.
type quietPresenter struct {
	stats *stats.Collector
}

func (p *quietPresenter) Run(events <-chan Event) error {
	for ev := range events {
		p.handleEvent(ev)
	}
	return nil
}

func (p *quietPresenter) handleEvent(ev Event) {
	// Quiet mode still needs to track scan totals for the collector.
	switch ev.Type {
	case ScanComplete:
		p.stats.SetTotals(ev.Total, ev.TotalSize)
	}
}

func (p *quietPresenter) Summary() string {
	return ""
}
