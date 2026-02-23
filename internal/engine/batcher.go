package engine

// BatchConfig controls small-file batching behavior.
type BatchConfig struct {
	MaxBytes  int64 // max total bytes per batch (default 4MB)
	MaxWait   int64 // max wait time in milliseconds before flushing a partial batch (default 50)
	SizeLimit int64 // max size of a single file eligible for batching (default 64KB)
	MaxCount  int   // max files per batch (default 100)
}

// DefaultBatchConfig returns the default batching configuration.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxCount:  100,
		MaxBytes:  4 * 1024 * 1024, // 4 MB
		MaxWait:   50,              // 50 ms
		SizeLimit: 64 * 1024,       // 64 KB
	}
}

// batcher accumulates small regular files into batches.
type batcher struct {
	pending  []FileTask
	cfg      BatchConfig
	curBytes int64
}

func newBatcher(cfg BatchConfig) *batcher {
	return &batcher{
		cfg:     cfg,
		pending: make([]FileTask, 0, cfg.MaxCount),
	}
}

// add attempts to add a task to the current batch. Returns true if the task
// was accepted (small regular file within limits), false if the task should
// be processed individually.
func (b *batcher) add(task FileTask) bool {
	if task.Type != Regular {
		return false
	}
	if task.Size > b.cfg.SizeLimit || task.Size < 0 {
		return false
	}
	if b.curBytes+task.Size > b.cfg.MaxBytes && len(b.pending) > 0 {
		return false
	}
	b.pending = append(b.pending, task)
	b.curBytes += task.Size
	return true
}

// ready returns true if the batch should be flushed (full count or full bytes).
func (b *batcher) ready() bool {
	return len(b.pending) >= b.cfg.MaxCount || b.curBytes >= b.cfg.MaxBytes
}

// len returns the number of pending tasks.
func (b *batcher) len() int {
	return len(b.pending)
}

// flush returns the pending tasks as a batch and resets the batcher.
func (b *batcher) flush() []FileTask {
	if len(b.pending) == 0 {
		return nil
	}
	batch := b.pending
	b.pending = make([]FileTask, 0, b.cfg.MaxCount)
	b.curBytes = 0
	return batch
}
