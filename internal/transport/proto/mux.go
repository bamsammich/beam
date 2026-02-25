package proto

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// StreamHandler is called when a frame arrives on an unregistered stream.
// The mux auto-creates the stream before calling the handler, so the handler
// can read subsequent frames from the channel. Called in its own goroutine.
type StreamHandler func(streamID uint32, ch <-chan Frame)

// Mux multiplexes multiple streams over a single connection.
// A single reader goroutine dispatches incoming frames to per-stream channels.
// A single writer goroutine serializes outgoing frames to the connection.
type Mux struct {
	conn       net.Conn
	err        error
	writeCh    chan Frame
	streams    map[uint32]chan Frame
	handler    StreamHandler
	done       chan struct{}
	shutdownCh chan struct{}
	mu         sync.Mutex
	closed     bool
}

// NewMux creates a new multiplexer wrapping the given connection.
// Call Run() to start the read/write loops.
// If the underlying connection supports it, TCP_NODELAY is set to avoid
// Nagle-algorithm delays on small frames (the buffered writer in writeLoop
// handles batching at the application level).
func NewMux(conn net.Conn) *Mux {
	// Set TCP_NODELAY — the buffered writer handles coalescing.
	//nolint:errcheck // best-effort; non-TCP connections may not support this
	if tc, ok := conn.(interface{ SetNoDelay(bool) error }); ok {
		tc.SetNoDelay(true)
	}
	return &Mux{
		conn:       conn,
		writeCh:    make(chan Frame, 256),
		streams:    make(map[uint32]chan Frame),
		done:       make(chan struct{}),
		shutdownCh: make(chan struct{}),
	}
}

// SetHandler sets a callback for frames arriving on unregistered streams.
// When a frame arrives on an unknown stream ID and a handler is set,
// the mux auto-creates the stream and calls the handler in a new goroutine.
// The first frame is already delivered to the channel before the handler starts.
// Must be called before Run().
func (m *Mux) SetHandler(h StreamHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = h
}

// OpenStream registers a stream and returns a channel for receiving frames.
// The caller must call CloseStream when done.
func (m *Mux) OpenStream(id uint32) <-chan Frame {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ch, ok := m.streams[id]; ok {
		return ch
	}

	ch := make(chan Frame, 64)
	m.streams[id] = ch
	return ch
}

// CloseStream unregisters a stream and closes its channel.
func (m *Mux) CloseStream(id uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ch, ok := m.streams[id]; ok {
		delete(m.streams, id)
		close(ch)
	}
}

// Send queues a frame for writing. Returns an error if the mux is closed.
//
// There is an inherent race between checking m.closed and sending on writeCh:
// Run() may close writeCh between our check and the channel send. The deferred
// recover catches the resulting panic without requiring the caller to hold the
// mutex across the channel operation (which would risk deadlock with writeLoop).
func (m *Mux) Send(f Frame) (sendErr error) {
	defer func() {
		if recover() != nil {
			sendErr = errors.New("mux closed")
		}
	}()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("mux closed")
	}
	m.mu.Unlock()

	select {
	case m.writeCh <- f:
		return nil
	case <-m.done:
		return errors.New("mux closed")
	}
}

// Run starts the reader and writer goroutines. Blocks until the connection
// is closed or an error occurs. Returns the first error.
func (m *Mux) Run() error {
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Writer goroutine: drain writeCh, serialize to conn.
	wg.Go(func() {
		errCh <- m.writeLoop()
	})

	// Reader goroutine: read frames, dispatch to stream channels.
	wg.Go(func() {
		errCh <- m.readLoop()
	})

	// Wait for first error from either loop.
	m.err = <-errCh

	// Signal shutdown: close conn to unblock both loops.
	m.conn.Close()
	close(m.done)

	// Close writeCh to unblock writer if it's still running.
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		close(m.writeCh)
	}
	m.mu.Unlock()

	wg.Wait()

	// Close all remaining stream channels so receivers unblock.
	m.mu.Lock()
	for id, ch := range m.streams {
		delete(m.streams, id)
		close(ch)
	}
	m.mu.Unlock()

	if errors.Is(m.err, io.EOF) || errors.Is(m.err, net.ErrClosed) {
		return nil
	}
	return m.err
}

// Close shuts down the mux by closing the underlying connection.
func (m *Mux) Close() error {
	return m.conn.Close()
}

// Shutdown gracefully stops the mux, draining any pending writes before
// closing. Unlike Close (which drops the connection immediately), Shutdown
// signals the writeLoop to flush remaining frames, then waits for Run() to
// complete. This ensures that frames queued via Send (e.g. an AuthResult
// rejection) reach the peer before the connection is torn down.
func (m *Mux) Shutdown() {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
	}
	m.mu.Unlock()

	close(m.shutdownCh)
	<-m.done
}

// Done returns a channel that is closed when the mux has stopped.
func (m *Mux) Done() <-chan struct{} {
	return m.done
}

func (m *Mux) readLoop() error {
	for {
		f, err := ReadFrame(m.conn)
		if err != nil {
			return fmt.Errorf("read frame: %w", err)
		}

		m.mu.Lock()
		ch, ok := m.streams[f.StreamID]
		handler := m.handler
		m.mu.Unlock()

		if ok {
			select {
			case ch <- f:
			case <-m.done:
				return nil
			}
			continue
		}

		// No registered stream. If there's a handler, auto-create the stream
		// and dispatch.
		if handler != nil {
			ch := make(chan Frame, 64)
			m.mu.Lock()
			m.streams[f.StreamID] = ch
			m.mu.Unlock()

			// Deliver the first frame.
			ch <- f

			go handler(f.StreamID, ch)
			continue
		}

		// No handler — discard (forward compatibility).
	}
}

//nolint:revive // cognitive-complexity: select-in-loop with shutdown handling is inherently branchy
func (m *Mux) writeLoop() error {
	bw := bufio.NewWriterSize(m.conn, 64*1024)
	for {
		select {
		case f, ok := <-m.writeCh:
			if !ok {
				// writeCh closed by Run() — flush and exit.
				return bw.Flush()
			}
			if err := WriteFrame(bw, f); err != nil {
				return fmt.Errorf("write frame: %w", err)
			}
			// Flush when the write channel is drained (no more frames queued),
			// so we batch multiple frames into fewer TCP segments.
			if len(m.writeCh) == 0 {
				if err := bw.Flush(); err != nil {
					return fmt.Errorf("flush: %w", err)
				}
			}
		case <-m.shutdownCh:
			return m.drainWrites(bw)
		}
	}
}

// drainWrites flushes remaining frames from writeCh during graceful shutdown.
func (m *Mux) drainWrites(bw *bufio.Writer) error {
	for {
		select {
		case f := <-m.writeCh:
			if err := WriteFrame(bw, f); err != nil {
				return fmt.Errorf("write frame: %w", err)
			}
		default:
			return bw.Flush()
		}
	}
}
