package proto

import (
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	compressNone byte = 0x00
	compressZstd byte = 0x01
)

// WriteFlusher is implemented by connections that buffer writes and require
// explicit flushing (e.g. compressed connections). The mux checks for this
// interface at construction time and uses it instead of bufio.Writer.
type WriteFlusher interface {
	Flush() error
}

// Releaser is implemented by connections that hold resources (e.g. codec
// goroutines) that must be freed after all I/O has stopped. The mux calls
// Release after its read/write goroutines have exited.
type Releaser interface {
	Release()
}

// compressedConn wraps a net.Conn with zstd streaming compression.
// Writes are compressed by the encoder; reads are decompressed by the decoder.
// Implements net.Conn and WriteFlusher.
//
// Close only shuts down the underlying conn (to unblock concurrent Read/Write),
// then releases encoder/decoder resources exactly once. The mutex serializes
// encoder operations (Write, Flush) to prevent data races during teardown.
type compressedConn struct {
	net.Conn
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	mu      sync.Mutex // protects encoder methods
	once    sync.Once  // ensures encoder/decoder cleanup runs once
}

// NewCompressedConn wraps a net.Conn with zstd streaming compression.
// The encoder uses level 1 (SpeedFastest) with single-threaded encoding.
func NewCompressedConn(conn net.Conn) (net.Conn, error) {
	encoder, err := zstd.NewWriter(conn,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(conn, zstd.WithDecoderConcurrency(1))
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("zstd decoder: %w", err)
	}

	return &compressedConn{
		Conn:    conn,
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (c *compressedConn) Read(p []byte) (int, error) {
	return c.decoder.Read(p)
}

func (c *compressedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.encoder.Write(p)
}

// Flush emits a syncable zstd frame so the decoder can consume buffered data
// immediately. Called by the mux writeLoop when the write channel drains.
func (c *compressedConn) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.encoder.Flush()
}

// Close shuts down the connection by closing the underlying conn to unblock
// any concurrent Read/Write calls. Codec resources (encoder/decoder) are
// not released here — call Release after all goroutines using the conn have
// stopped (e.g. after mux.Run returns).
func (c *compressedConn) Close() error {
	return c.Conn.Close()
}

// Release frees the zstd encoder and decoder resources. Must be called after
// all concurrent Read/Write/Flush calls have stopped (i.e. after the mux's
// Run method returns). Safe to call multiple times.
func (c *compressedConn) Release() {
	c.once.Do(func() {
		c.mu.Lock()
		c.encoder.Close()
		c.mu.Unlock()
		c.decoder.Close()
	})
}

// NegotiateCompression performs the client side of compression negotiation.
// Sends a 1-byte preference, reads the server's 1-byte response.
// On success returns the original conn (no compression) or a compressedConn.
// On error returns nil; the caller must close the original conn.
func NegotiateCompression(conn net.Conn, wantCompress bool) (net.Conn, error) {
	req := compressNone
	if wantCompress {
		req = compressZstd
	}

	if _, err := conn.Write([]byte{req}); err != nil {
		return nil, fmt.Errorf("write compression preference: %w", err)
	}

	var resp [1]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		return nil, fmt.Errorf("read compression response: %w", err)
	}

	if resp[0] == compressZstd {
		return NewCompressedConn(conn)
	}
	return conn, nil
}

// AcceptCompression performs the server side of compression negotiation.
// Reads the client's 1-byte preference, echoes agreement.
// Returns the original conn (no compression) or a compressedConn.
func AcceptCompression(conn net.Conn) (net.Conn, error) {
	var req [1]byte
	if _, err := io.ReadFull(conn, req[:]); err != nil {
		return nil, fmt.Errorf("read compression request: %w", err)
	}

	resp := req[0] // echo: agree to whatever client requested
	if _, err := conn.Write([]byte{resp}); err != nil {
		return nil, fmt.Errorf("write compression response: %w", err)
	}

	if resp == compressZstd {
		return NewCompressedConn(conn)
	}
	return conn, nil
}
