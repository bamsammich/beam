package engine

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zeebo/blake3"
	_ "modernc.org/sqlite"
)

// CheckpointDB provides SQLite-backed resume state for interrupted copies.
type CheckpointDB struct {
	db   *sql.DB
	path string

	// Batch buffer for MarkCompleted calls.
	mu      sync.Mutex
	batch   []completedEntry
	done    chan struct{}
	stopped bool
}

type completedEntry struct {
	relPath   string
	size      int64
	hash      string
	mtimeNano int64
}

// OpenCheckpoint opens (or creates) a checkpoint database for the given
// source/destination pair. The DB is stored at
// $XDG_RUNTIME_DIR/beam/<job-id>.db or /tmp/beam-<job-id>.db.
func OpenCheckpoint(src, dst string) (*CheckpointDB, error) {
	jobID := checkpointJobID(src, dst)
	dbPath := checkpointPath(jobID)

	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open checkpoint db: %w", err)
	}

	c := &CheckpointDB{
		db:   db,
		path: dbPath,
		done: make(chan struct{}),
	}

	if err := c.init(src, dst); err != nil {
		db.Close()
		return nil, err
	}

	// Start background batch flusher.
	go c.flushLoop()

	return c, nil
}

func (c *CheckpointDB) init(src, dst string) error {
	_, err := c.db.Exec(`
		CREATE TABLE IF NOT EXISTS completed (
			path    TEXT PRIMARY KEY,
			size    INTEGER NOT NULL,
			hash    TEXT NOT NULL,
			mtime   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	// Validate or store src/dst roots.
	var storedSrc, storedDst string
	row := c.db.QueryRow("SELECT value FROM meta WHERE key = 'src_root'")
	if err := row.Scan(&storedSrc); err == nil {
		// Existing DB — validate roots match.
		row2 := c.db.QueryRow("SELECT value FROM meta WHERE key = 'dst_root'")
		if err := row2.Scan(&storedDst); err == nil {
			if storedSrc != src || storedDst != dst {
				return fmt.Errorf("checkpoint roots mismatch: stored %s->%s, got %s->%s",
					storedSrc, storedDst, src, dst)
			}
		}
	} else {
		// New DB — store roots.
		_, err = c.db.Exec("INSERT OR REPLACE INTO meta (key, value) VALUES ('src_root', ?), ('dst_root', ?)", src, dst)
		if err != nil {
			return fmt.Errorf("store meta: %w", err)
		}
	}

	return nil
}

// IsCompleted returns true if the given file (by relative path, size, and mtime)
// is recorded as already copied.
func (c *CheckpointDB) IsCompleted(relPath string, size int64, mtimeNano int64) bool {
	var storedSize, storedMtime int64
	err := c.db.QueryRow(
		"SELECT size, mtime FROM completed WHERE path = ?", relPath,
	).Scan(&storedSize, &storedMtime)
	if err != nil {
		return false
	}
	return storedSize == size && storedMtime == mtimeNano
}

// MarkCompleted records a file as successfully copied. Writes are batched
// and flushed periodically for performance.
func (c *CheckpointDB) MarkCompleted(relPath string, size int64, hash string, mtimeNano int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.batch = append(c.batch, completedEntry{
		relPath:   relPath,
		size:      size,
		hash:      hash,
		mtimeNano: mtimeNano,
	})

	if len(c.batch) >= 100 {
		return c.flushLocked()
	}
	return nil
}

// Flush writes any pending batch entries to the database.
func (c *CheckpointDB) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushLocked()
}

func (c *CheckpointDB) flushLocked() error {
	if len(c.batch) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO completed (path, size, hash, mtime) VALUES (?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, e := range c.batch {
		if _, err := stmt.Exec(e.relPath, e.size, e.hash, e.mtimeNano); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert %s: %w", e.relPath, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	c.batch = c.batch[:0]
	return nil
}

func (c *CheckpointDB) flushLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.mu.Lock()
			_ = c.flushLocked()
			c.mu.Unlock()
		}
	}
}

// Close flushes any pending writes and closes the database.
func (c *CheckpointDB) Close() error {
	c.mu.Lock()
	if !c.stopped {
		c.stopped = true
		close(c.done)
	}
	_ = c.flushLocked()
	c.mu.Unlock()
	return c.db.Close()
}

// Remove deletes the checkpoint database file.
func (c *CheckpointDB) Remove() error {
	return os.Remove(c.path)
}

// Path returns the path to the checkpoint database file.
func (c *CheckpointDB) Path() string {
	return c.path
}

// checkpointJobID computes a deterministic job ID from source and destination paths.
func checkpointJobID(src, dst string) string {
	h := blake3.New()
	h.Write([]byte(src))
	h.Write([]byte{0})
	h.Write([]byte(dst))
	digest := h.Sum(nil)
	return hex.EncodeToString(digest[:8])
}

// checkpointPath returns the filesystem path for a checkpoint DB.
func checkpointPath(jobID string) string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "beam", jobID+".db")
	}
	return filepath.Join(os.TempDir(), "beam-"+jobID+".db")
}
