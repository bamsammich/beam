package engine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
)

// VerifyConfig controls the post-copy verification pass.
type VerifyConfig struct {
	SrcRoot string
	DstRoot string
	Workers int
	Filter  *filter.Chain
	Events  chan<- event.Event
	Stats   stats.Writer
}

// VerifyResult holds the outcome of a verification pass.
type VerifyResult struct {
	Verified int64
	Failed   int64
	Errors   []VerifyError
}

// VerifyError records a single checksum mismatch.
type VerifyError struct {
	Path    string
	SrcHash string
	DstHash string
}

// Verify walks the destination tree and compares BLAKE3 checksums against
// the source for every regular file. It fans out to cfg.Workers goroutines.
func Verify(ctx context.Context, cfg VerifyConfig) VerifyResult {
	emitEvent(cfg.Events, event.Event{Type: event.VerifyStarted})

	workers := cfg.Workers
	if workers <= 0 {
		workers = 4
	}

	// Collect files to verify by walking destination.
	files := collectVerifyFiles(ctx, cfg.DstRoot, cfg.SrcRoot, cfg.Filter)

	// Fan out to workers.
	taskCh := make(chan string, workers*2)
	var mu sync.Mutex
	var result VerifyResult
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for relPath := range taskCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				srcPath := filepath.Join(cfg.SrcRoot, relPath)
				dstPath := filepath.Join(cfg.DstRoot, relPath)

				srcHash, err := HashFile(srcPath)
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

				dstHash, err := HashFile(dstPath)
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
		select {
		case <-ctx.Done():
			break
		case taskCh <- f:
		}
	}
	close(taskCh)
	wg.Wait()

	return result
}

// collectVerifyFiles walks the destination tree and returns relative paths
// of regular files that pass the filter.
func collectVerifyFiles(ctx context.Context, dstRoot, srcRoot string, f *filter.Chain) []string {
	var files []string
	_ = filepath.WalkDir(dstRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}

		relPath, err := filepath.Rel(dstRoot, path)
		if err != nil {
			return nil
		}

		// Apply filter if configured.
		if f != nil {
			info, err := d.Info()
			if err != nil {
				return nil
			}
			if !f.Match(relPath, false, info.Size()) {
				return nil
			}
		}

		// Only verify files that also exist in source.
		srcPath := filepath.Join(srcRoot, relPath)
		if _, err := os.Lstat(srcPath); err != nil {
			return nil
		}

		files = append(files, relPath)
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
