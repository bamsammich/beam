package proto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// FrameHeaderSize is the size of the frame header in bytes:
	// 4 bytes frame length + 4 bytes stream ID + 1 byte message type.
	FrameHeaderSize = 9

	// MaxFrameSize is the maximum allowed frame size (including header).
	MaxFrameSize = 4 * 1024 * 1024 // 4 MB

	// DataChunkSize is the size of data chunks for file streaming.
	DataChunkSize = 256 * 1024 // 256 KB

	// ControlStream is the stream ID for control messages (handshake, ping).
	ControlStream uint32 = 0
)

// Frame is a single protocol message on the wire.
type Frame struct {
	StreamID uint32
	MsgType  byte
	Payload  []byte
}

// ErrFrameTooLarge is returned when a frame exceeds MaxFrameSize.
var ErrFrameTooLarge = errors.New("frame exceeds maximum size")

// WriteFrame writes a length-prefixed frame to w.
// Wire format: [4-byte length (big-endian)][4-byte stream ID][1-byte msg type][payload]
// The length field includes the header (stream ID + msg type) and payload.
func WriteFrame(w io.Writer, f Frame) error {
	totalLen := uint32(4 + 1 + len(f.Payload)) // stream ID + msg type + payload
	if totalLen+4 > MaxFrameSize {
		return ErrFrameTooLarge
	}

	var header [FrameHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], totalLen)
	binary.BigEndian.PutUint32(header[4:8], f.StreamID)
	header[8] = f.MsgType

	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return fmt.Errorf("write frame payload: %w", err)
		}
	}
	return nil
}

// ReadFrame reads a length-prefixed frame from r.
func ReadFrame(r io.Reader) (Frame, error) {
	var header [FrameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}

	totalLen := binary.BigEndian.Uint32(header[0:4])
	if totalLen+4 > MaxFrameSize {
		return Frame{}, ErrFrameTooLarge
	}
	if totalLen < 5 {
		return Frame{}, fmt.Errorf("frame too small: length %d", totalLen)
	}

	f := Frame{
		StreamID: binary.BigEndian.Uint32(header[4:8]),
		MsgType:  header[8],
	}

	payloadLen := totalLen - 5 // subtract stream ID (4) + msg type (1)
	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return Frame{}, fmt.Errorf("read frame payload: %w", err)
		}
	}

	return f, nil
}
