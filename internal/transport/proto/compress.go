package proto

import (
	"fmt"
	"net"

	"github.com/klauspost/compress/zstd"
)

// WriteFlusher is implemented by connections that buffer writes and require
// explicit flushing (e.g. compressed connections). The mux checks for this
// interface at construction time and uses it instead of bufio.Writer.
type WriteFlusher interface {
	Flush() error
}

// compressedConn wraps a net.Conn with zstd streaming compression.
// Writes are compressed by the encoder; reads are decompressed by the decoder.
// Implements net.Conn and WriteFlusher.
type compressedConn struct {
	net.Conn
	encoder *zstd.Encoder
	decoder *zstd.Decoder
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

	decoder, err := zstd.NewReader(conn)
	if err != nil {
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
	return c.encoder.Write(p)
}

// Flush emits a syncable zstd frame so the decoder can consume buffered data
// immediately. Called by the mux writeLoop when the write channel drains.
func (c *compressedConn) Flush() error {
	return c.encoder.Flush()
}

// Close shuts down the encoder, closes the underlying conn (to unblock the
// decoder's background reader goroutine), then releases the decoder.
func (c *compressedConn) Close() error {
	c.encoder.Close()
	err := c.Conn.Close()
	c.decoder.Close()
	return err
}
