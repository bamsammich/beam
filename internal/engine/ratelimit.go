package engine

import (
	"context"
	"io"

	"golang.org/x/time/rate"
)

// NewBWLimiter creates a rate.Limiter that caps aggregate throughput to
// bytesPerSec. The burst is set to 1 MB to allow natural read-size chunks
// through without unnecessary blocking on small reads.
func NewBWLimiter(bytesPerSec int64) *rate.Limiter {
	burst := 1 << 20 // 1 MB
	if bytesPerSec < int64(burst) {
		burst = int(bytesPerSec)
	}
	return rate.NewLimiter(rate.Limit(bytesPerSec), burst)
}

// rateLimitedReader wraps an io.Reader and enforces a shared rate limit.
type rateLimitedReader struct {
	r       io.Reader
	limiter *rate.Limiter
	ctx     context.Context
}

// newRateLimitedReader wraps r so that reads are throttled by limiter.
func newRateLimitedReader(
	ctx context.Context,
	r io.Reader,
	limiter *rate.Limiter,
) *rateLimitedReader {
	return &rateLimitedReader{r: r, limiter: limiter, ctx: ctx}
}

func (rl *rateLimitedReader) Read(p []byte) (int, error) {
	n, err := rl.r.Read(p)
	if n > 0 {
		if waitErr := rl.limiter.WaitN(rl.ctx, n); waitErr != nil {
			return n, waitErr
		}
	}
	return n, err
}

// rateLimitedWriter wraps an io.Writer and enforces a shared rate limit.
type rateLimitedWriter struct {
	w       io.Writer
	limiter *rate.Limiter
	ctx     context.Context
}

func (rw *rateLimitedWriter) Write(p []byte) (int, error) {
	if err := rw.limiter.WaitN(rw.ctx, len(p)); err != nil {
		return 0, err
	}
	return rw.w.Write(p)
}
