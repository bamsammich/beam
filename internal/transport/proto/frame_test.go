package proto_test

import (
	"bytes"
	"testing"

	"github.com/bamsammich/beam/internal/transport/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		frame proto.Frame
	}{
		{
			name: "control frame with payload",
			frame: proto.Frame{
				StreamID: 0,
				MsgType:  proto.MsgHandshakeReq,
				Payload:  []byte("hello"),
			},
		},
		{
			name: "worker frame with data",
			frame: proto.Frame{
				StreamID: 42,
				MsgType:  proto.MsgWriteData,
				Payload:  bytes.Repeat([]byte("x"), 1024),
			},
		},
		{
			name: "empty payload",
			frame: proto.Frame{
				StreamID: 1,
				MsgType:  proto.MsgAckResp,
				Payload:  nil,
			},
		},
		{
			name: "large payload",
			frame: proto.Frame{
				StreamID: 5,
				MsgType:  proto.MsgReadData,
				Payload:  bytes.Repeat([]byte("a"), proto.DataChunkSize),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			err := proto.WriteFrame(&buf, tt.frame)
			require.NoError(t, err)

			got, err := proto.ReadFrame(&buf)
			require.NoError(t, err)

			assert.Equal(t, tt.frame.StreamID, got.StreamID)
			assert.Equal(t, tt.frame.MsgType, got.MsgType)
			assert.Equal(t, tt.frame.Payload, got.Payload)
		})
	}
}

func TestFrameOversizedRejected(t *testing.T) {
	t.Parallel()

	// A frame with payload exceeding MaxFrameSize should fail on write.
	f := proto.Frame{
		StreamID: 1,
		MsgType:  proto.MsgReadData,
		Payload:  make([]byte, proto.MaxFrameSize), // payload alone exceeds max
	}

	var buf bytes.Buffer
	err := proto.WriteFrame(&buf, f)
	assert.ErrorIs(t, err, proto.ErrFrameTooLarge)
}

func TestFrameMultipleRoundTrips(t *testing.T) {
	t.Parallel()

	frames := []proto.Frame{
		{StreamID: 0, MsgType: proto.MsgHandshakeReq, Payload: []byte("req")},
		{StreamID: 0, MsgType: proto.MsgHandshakeResp, Payload: []byte("resp")},
		{StreamID: 1, MsgType: proto.MsgStatReq, Payload: []byte("stat")},
	}

	var buf bytes.Buffer
	for _, f := range frames {
		require.NoError(t, proto.WriteFrame(&buf, f))
	}

	for _, want := range frames {
		got, err := proto.ReadFrame(&buf)
		require.NoError(t, err)
		assert.Equal(t, want.StreamID, got.StreamID)
		assert.Equal(t, want.MsgType, got.MsgType)
		assert.Equal(t, want.Payload, got.Payload)
	}
}
