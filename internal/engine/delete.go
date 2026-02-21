package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
)

// DeleteConfig controls the delete pass.
type DeleteConfig struct {
	SrcRoot string
	DstRoot string
	Filter  *filter.Chain
	DryRun  bool
	Events  chan<- event.Event
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
func DeleteExtraneous(ctx context.Context, cfg DeleteConfig) (int, error) {
	var toDelete []string // files
	var dirsToDelete []string

	err := filepath.Walk(cfg.DstRoot, func(dstPath string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip inaccessible entries
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		relPath, err := filepath.Rel(cfg.DstRoot, dstPath)
		if err != nil || relPath == "." {
			return nil
		}

		srcPath := filepath.Join(cfg.SrcRoot, relPath)

		// If filter excludes this path, don't delete it (it was intentionally not copied).
		if cfg.Filter != nil && !cfg.Filter.Match(relPath, info.IsDir(), info.Size()) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Check if the corresponding source exists.
		if _, err := os.Lstat(srcPath); err == nil {
			return nil // source exists, keep it
		}

		// Source doesn't exist — mark for deletion.
		if info.IsDir() {
			dirsToDelete = append(dirsToDelete, dstPath)
			return filepath.SkipDir // don't recurse into dirs we'll delete
		}

		toDelete = append(toDelete, dstPath)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk destination for delete: %w", err)
	}

	deleted := 0

	// Delete files first.
	for _, path := range toDelete {
		relPath, _ := filepath.Rel(cfg.DstRoot, path)
		cfg.emit(event.Event{Type: event.DeleteFile, Path: relPath})

		if !cfg.DryRun {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return deleted, fmt.Errorf("delete %s: %w", path, err)
			}
		}
		deleted++
	}

	// Delete directories bottom-up (deepest first).
	sort.Sort(sort.Reverse(sort.StringSlice(dirsToDelete)))
	for _, path := range dirsToDelete {
		relPath, _ := filepath.Rel(cfg.DstRoot, path)
		cfg.emit(event.Event{Type: event.DeleteFile, Path: relPath})

		if !cfg.DryRun {
			if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
				return deleted, fmt.Errorf("delete dir %s: %w", path, err)
			}
		}
		deleted++
	}

	return deleted, nil
}
