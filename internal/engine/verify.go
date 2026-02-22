package engine

import (
	"context"
	"sync"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
)

// VerifyConfig controls the post-copy verification pass.
type VerifyConfig struct {
	Events      chan<- event.Event
	Stats       stats.Writer
	SrcEndpoint transport.ReadEndpoint
	DstEndpoint transport.WriteEndpoint
	Filter      *filter.Chain
	SrcRoot     string
	DstRoot     string
	Workers     int
}

// VerifyResult holds the outcome of a verification pass.
type VerifyResult struct {
	Errors   []VerifyError
	Verified int64
	Failed   int64
}

// VerifyError records a single checksum mismatch.
type VerifyError struct {
	Path    string
	SrcHash string
	DstHash string
}

// Verify walks the destination tree and compares BLAKE3 checksums against
// the source for every regular file. It fans out to cfg.Workers goroutines.
// SrcEndpoint and DstEndpoint must be set by the caller.
//
//nolint:revive // cognitive-complexity: hash comparison with error handling for src/dst mismatch
func Verify(ctx context.Context, cfg VerifyConfig) VerifyResult {
	emitEvent(cfg.Events, event.Event{Type: event.VerifyStarted})

	workers := cfg.Workers
	if workers <= 0 {
		workers = 4
	}

	// Collect files to verify by walking destination.
	files := collectVerifyFiles(ctx, cfg)

	// Fan out to workers.
	taskCh := make(chan string, workers*2)
	var mu sync.Mutex
	var result VerifyResult
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1) //nolint:revive // use-waitgroup-go: WaitGroup.Go not available in Go 1.25
		go func() {
			defer wg.Done()
			for relPath := range taskCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				srcHash, err := cfg.SrcEndpoint.Hash(relPath)
				if err != nil {
					// Source missing or unreadable — treat as mismatch.
					mu.Lock()
					result.Failed++
					result.Errors = append(result.Errors, VerifyError{
						Path:    relPath,
						SrcHash: "error",
						DstHash: "n/a",
					})
					mu.Unlock()
					cfg.Stats.AddFilesVerifyFailed(1)
					emitEvent(cfg.Events, event.Event{
						Type:  event.VerifyFailed,
						Path:  relPath,
						Error: err,
					})
					continue
				}

				dstHash, err := cfg.DstEndpoint.Hash(relPath)
				if err != nil {
					mu.Lock()
					result.Failed++
					result.Errors = append(result.Errors, VerifyError{
						Path:    relPath,
						SrcHash: srcHash,
						DstHash: "error",
					})
					mu.Unlock()
					cfg.Stats.AddFilesVerifyFailed(1)
					emitEvent(cfg.Events, event.Event{
						Type:  event.VerifyFailed,
						Path:  relPath,
						Error: err,
					})
					continue
				}

				if srcHash != dstHash {
					mu.Lock()
					result.Failed++
					result.Errors = append(result.Errors, VerifyError{
						Path:    relPath,
						SrcHash: srcHash,
						DstHash: dstHash,
					})
					mu.Unlock()
					cfg.Stats.AddFilesVerifyFailed(1)
					emitEvent(cfg.Events, event.Event{
						Type: event.VerifyFailed,
						Path: relPath,
					})
				} else {
					mu.Lock()
					result.Verified++
					mu.Unlock()
					cfg.Stats.AddFilesVerified(1)
					emitEvent(cfg.Events, event.Event{
						Type: event.VerifyOK,
						Path: relPath,
					})
				}
			}
		}()
	}

	for _, f := range files {
		if ctx.Err() != nil {
			break
		}
		taskCh <- f
	}
	close(taskCh)
	wg.Wait()

	return result
}

// collectVerifyFiles walks the destination tree and returns relative paths
// of regular files that pass the filter and also exist in the source.
//
//nolint:revive // cognitive-complexity: walk callback with filter + source existence check
func collectVerifyFiles(ctx context.Context, cfg VerifyConfig) []string {
	var files []string
	//nolint:errcheck // walk errors are non-fatal for verification file collection
	_ = cfg.DstEndpoint.Walk(func(entry transport.FileEntry) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if entry.IsDir || entry.IsSymlink {
			return nil
		}
		if !entry.Mode.IsRegular() {
			return nil
		}

		// Apply filter if configured.
		if cfg.Filter != nil {
			if !cfg.Filter.Match(entry.RelPath, false, entry.Size) {
				return nil
			}
		}

		// Only verify files that also exist in source.
		if _, err := cfg.SrcEndpoint.Stat(entry.RelPath); err != nil {
			return nil
		}

		files = append(files, entry.RelPath)
		return nil
	})
	return files
}

func emitEvent(ch chan<- event.Event, e event.Event) {
	if ch == nil {
		return
	}
	e.Timestamp = time.Now()
	select {
	case ch <- e:
	default:
	}
}
