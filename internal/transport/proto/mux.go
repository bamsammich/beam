package proto

import (
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
	conn net.Conn

	writeCh chan Frame // outgoing frames serialized by writer goroutine

	mu      sync.Mutex
	streams map[uint32]chan Frame // stream ID → incoming frame channel
	closed  bool
	handler StreamHandler // called for frames on unregistered streams

	done chan struct{} // closed when Run exits
	err  error        // first error from reader or writer
}

// NewMux creates a new multiplexer wrapping the given connection.
// Call Run() to start the read/write loops.
func NewMux(conn net.Conn) *Mux {
	return &Mux{
		conn:    conn,
		writeCh: make(chan Frame, 256),
		streams: make(map[uint32]chan Frame),
		done:    make(chan struct{}),
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
func (m *Mux) Send(f Frame) error {
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

	wg.Add(2)

	// Writer goroutine: drain writeCh, serialize to conn.
	go func() {
		defer wg.Done()
		errCh <- m.writeLoop()
	}()

	// Reader goroutine: read frames, dispatch to stream channels.
	go func() {
		defer wg.Done()
		errCh <- m.readLoop()
	}()

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

func (m *Mux) writeLoop() error {
	for f := range m.writeCh {
		if err := WriteFrame(m.conn, f); err != nil {
			return fmt.Errorf("write frame: %w", err)
		}
	}
	return nil
}
