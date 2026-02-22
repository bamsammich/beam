package engine

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/transport"
)

// DeleteConfig controls the delete pass.
type DeleteConfig struct {
	Events      chan<- event.Event
	SrcEndpoint transport.ReadEndpoint
	DstEndpoint transport.WriteEndpoint
	Filter      *filter.Chain
	SrcRoot     string
	DstRoot     string
	DryRun      bool
}

func (c DeleteConfig) emit(e event.Event) {
	if c.Events == nil {
		return
	}
	e.Timestamp = time.Now()
	select {
	case c.Events <- e:
	default:
	}
}

// DeleteExtraneous removes files/directories from DstRoot that don't exist
// in SrcRoot. Returns the number of items deleted and any error.
// SrcEndpoint and DstEndpoint must be set by the caller.
//
//nolint:revive // cognitive-complexity: inherently sequential walk-filter-delete logic
func DeleteExtraneous(ctx context.Context, cfg DeleteConfig) (int, error) {
	var toDelete []string
	var dirsToDelete []string

	err := cfg.DstEndpoint.Walk(func(entry transport.FileEntry) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		relPath := entry.RelPath

		// If filter excludes this path, don't delete it (it was intentionally not copied).
		if cfg.Filter != nil && !cfg.Filter.Match(relPath, entry.IsDir, entry.Size) {
			return nil
		}

		// Check if the corresponding source exists.
		if _, err := cfg.SrcEndpoint.Stat(relPath); err == nil {
			return nil // source exists, keep it
		}

		// Source doesn't exist — mark for deletion.
		if entry.IsDir {
			dirsToDelete = append(dirsToDelete, relPath)
		} else {
			toDelete = append(toDelete, relPath)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk destination for delete: %w", err)
	}

	deleted := 0

	// Delete files first.
	for _, relPath := range toDelete {
		cfg.emit(event.Event{Type: event.DeleteFile, Path: relPath})

		if !cfg.DryRun {
			if err := cfg.DstEndpoint.Remove(relPath); err != nil && !os.IsNotExist(err) {
				return deleted, fmt.Errorf("delete %s: %w", relPath, err)
			}
		}
		deleted++
	}

	// Delete directories bottom-up (deepest first).
	sort.Sort(sort.Reverse(sort.StringSlice(dirsToDelete)))
	for _, relPath := range dirsToDelete {
		cfg.emit(event.Event{Type: event.DeleteFile, Path: relPath})

		if !cfg.DryRun {
			if err := cfg.DstEndpoint.RemoveAll(relPath); err != nil && !os.IsNotExist(err) {
				return deleted, fmt.Errorf("delete dir %s: %w", relPath, err)
			}
		}
		deleted++
	}

	return deleted, nil
}
