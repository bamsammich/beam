package engine

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBWLimiter(t *testing.T) {
	t.Parallel()

	t.Run("burst capped to rate when rate < 1MB", func(t *testing.T) {
		t.Parallel()
		lim := NewBWLimiter(1024)
		assert.Equal(t, 1024, lim.Burst())
	})

	t.Run("burst is 1MB when rate >= 1MB", func(t *testing.T) {
		t.Parallel()
		lim := NewBWLimiter(10 * 1024 * 1024)
		assert.Equal(t, 1<<20, lim.Burst())
	})
}

func TestRateLimitedReader(t *testing.T) {
	t.Parallel()

	t.Run("reads all data", func(t *testing.T) {
		t.Parallel()
		data := bytes.Repeat([]byte("x"), 4096)
		src := bytes.NewReader(data)
		lim := NewBWLimiter(1 << 20) // 1 MB/s — fast enough to not slow test
		rl := newRateLimitedReader(context.Background(), src, lim)

		got, err := io.ReadAll(rl)
		require.NoError(t, err)
		assert.Equal(t, data, got)
	})

	t.Run("enforces rate limit", func(t *testing.T) {
		t.Parallel()
		// 10 KB at 5 KB/s should take ~2s (minus burst).
		dataSize := 10 * 1024
		rateLimit := int64(5 * 1024)
		data := bytes.Repeat([]byte("a"), dataSize)
		src := bytes.NewReader(data)
		lim := NewBWLimiter(rateLimit)

		start := time.Now()
		rl := newRateLimitedReader(context.Background(), src, lim)
		got, err := io.ReadAll(rl)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Len(t, got, dataSize)
		// Should take at least 500ms (burst absorbs first chunk, then rate kicks in).
		assert.Greater(t, elapsed, 500*time.Millisecond,
			"rate limiter should slow reads to ~5KB/s")
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		t.Parallel()
		data := bytes.Repeat([]byte("b"), 1<<20) // 1 MB
		src := bytes.NewReader(data)
		lim := NewBWLimiter(1024) // 1 KB/s — very slow

		ctx, cancel := context.WithCancel(context.Background())
		rl := newRateLimitedReader(ctx, src, lim)

		// Cancel immediately — WaitN should return quickly with error.
		cancel()
		buf := make([]byte, 4096)
		// First read may succeed (burst), subsequent should fail.
		for range 100 {
			_, err := rl.Read(buf)
			if err != nil {
				return // context error is expected
			}
		}
		t.Fatal("expected context cancellation error")
	})
}
