package proto_test

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bamsammich/beam/internal/transport/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMuxSingleStream(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()

	clientMux := proto.NewMux(clientConn)
	serverMux := proto.NewMux(serverConn)

	// Start both muxes in background.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		clientMux.Run()
	}()
	go func() {
		defer wg.Done()
		serverMux.Run()
	}()

	// Open stream on both sides.
	clientCh := clientMux.OpenStream(1)
	serverCh := serverMux.OpenStream(1)

	// Client sends, server receives.
	payload := []byte("hello from client")
	require.NoError(t, clientMux.Send(proto.Frame{
		StreamID: 1,
		MsgType:  proto.MsgStatReq,
		Payload:  payload,
	}))

	select {
	case f := <-serverCh:
		assert.Equal(t, uint32(1), f.StreamID)
		assert.Equal(t, proto.MsgStatReq, f.MsgType)
		assert.Equal(t, payload, f.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for frame on server")
	}

	// Server sends, client receives.
	respPayload := []byte("response from server")
	require.NoError(t, serverMux.Send(proto.Frame{
		StreamID: 1,
		MsgType:  proto.MsgStatResp,
		Payload:  respPayload,
	}))

	select {
	case f := <-clientCh:
		assert.Equal(t, uint32(1), f.StreamID)
		assert.Equal(t, proto.MsgStatResp, f.MsgType)
		assert.Equal(t, respPayload, f.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for frame on client")
	}

	// Clean up.
	clientMux.CloseStream(1)
	serverMux.CloseStream(1)
	clientMux.Close()
	serverMux.Close()
	wg.Wait()
}

func TestMuxMultipleStreams(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()

	clientMux := proto.NewMux(clientConn)
	serverMux := proto.NewMux(serverConn)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		clientMux.Run()
	}()
	go func() {
		defer wg.Done()
		serverMux.Run()
	}()

	// Open multiple streams on server side.
	ch1 := serverMux.OpenStream(1)
	ch2 := serverMux.OpenStream(2)
	ch3 := serverMux.OpenStream(3)

	// Client sends on different streams concurrently.
	const numMessages = 10
	var sendWg sync.WaitGroup
	for _, streamID := range []uint32{1, 2, 3} {
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()
			for i := range numMessages {
				_ = clientMux.Send(proto.Frame{
					StreamID: streamID,
					MsgType:  proto.MsgStatReq,
					Payload:  []byte{byte(i)},
				})
			}
		}()
	}
	sendWg.Wait()

	// Collect from each stream.
	collectFromStream := func(ch <-chan proto.Frame, count int) []proto.Frame {
		var frames []proto.Frame
		for range count {
			select {
			case f := <-ch:
				frames = append(frames, f)
			case <-time.After(2 * time.Second):
				t.Error("timeout collecting frames")
				return frames
			}
		}
		return frames
	}

	frames1 := collectFromStream(ch1, numMessages)
	frames2 := collectFromStream(ch2, numMessages)
	frames3 := collectFromStream(ch3, numMessages)

	assert.Len(t, frames1, numMessages)
	assert.Len(t, frames2, numMessages)
	assert.Len(t, frames3, numMessages)

	// Verify stream IDs are correct.
	for _, f := range frames1 {
		assert.Equal(t, uint32(1), f.StreamID)
	}
	for _, f := range frames2 {
		assert.Equal(t, uint32(2), f.StreamID)
	}
	for _, f := range frames3 {
		assert.Equal(t, uint32(3), f.StreamID)
	}

	serverMux.CloseStream(1)
	serverMux.CloseStream(2)
	serverMux.CloseStream(3)
	clientMux.Close()
	serverMux.Close()
	wg.Wait()
}

func TestMuxUnknownStreamDiscarded(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()

	clientMux := proto.NewMux(clientConn)
	serverMux := proto.NewMux(serverConn)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		clientMux.Run()
	}()
	go func() {
		defer wg.Done()
		serverMux.Run()
	}()

	// Only register stream 1 on server.
	ch1 := serverMux.OpenStream(1)

	// Send to unregistered stream 99 — should be discarded.
	require.NoError(t, clientMux.Send(proto.Frame{
		StreamID: 99,
		MsgType:  proto.MsgStatReq,
		Payload:  []byte("discarded"),
	}))

	// Send to registered stream 1 — should arrive.
	require.NoError(t, clientMux.Send(proto.Frame{
		StreamID: 1,
		MsgType:  proto.MsgStatReq,
		Payload:  []byte("received"),
	}))

	select {
	case f := <-ch1:
		assert.Equal(t, []byte("received"), f.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for frame")
	}

	serverMux.CloseStream(1)
	clientMux.Close()
	serverMux.Close()
	wg.Wait()
}

func TestMuxSendAfterClose(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()

	clientMux := proto.NewMux(clientConn)
	serverMux := proto.NewMux(serverConn)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		clientMux.Run()
	}()
	go func() {
		defer wg.Done()
		serverMux.Run()
	}()

	clientMux.Close()
	wg.Wait()

	// Sending after close should return an error.
	err := clientMux.Send(proto.Frame{
		StreamID: 1,
		MsgType:  proto.MsgStatReq,
		Payload:  []byte("too late"),
	})
	assert.Error(t, err)
}
