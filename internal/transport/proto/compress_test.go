package proto_test

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/transport/proto"
)

func TestCompressedConnRoundTrip(t *testing.T) {
	t.Parallel()

	clientRaw, serverRaw := net.Pipe()

	clientConn, err := proto.NewCompressedConn(clientRaw)
	require.NoError(t, err)
	serverConn, err := proto.NewCompressedConn(serverRaw)
	require.NoError(t, err)

	msg := []byte("hello compressed world")

	var wg sync.WaitGroup
	wg.Go(func() {
		_, writeErr := clientConn.Write(msg)
		require.NoError(t, writeErr)
		wf, ok := clientConn.(proto.WriteFlusher)
		assert.True(t, ok, "clientConn must implement WriteFlusher")
		if ok {
			assert.NoError(t, wf.Flush())
		}
	})

	buf := make([]byte, len(msg))
	_, err = io.ReadFull(serverConn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	wg.Wait()
	clientConn.Close()
	serverConn.Close()
	clientConn.(proto.Releaser).Release()
	serverConn.(proto.Releaser).Release()
}

func TestCompressedConnLargePayload(t *testing.T) {
	t.Parallel()

	clientRaw, serverRaw := net.Pipe()

	clientConn, err := proto.NewCompressedConn(clientRaw)
	require.NoError(t, err)
	serverConn, err := proto.NewCompressedConn(serverRaw)
	require.NoError(t, err)

	// 1 MB of compressible data
	data := bytes.Repeat([]byte("beam compression test data "), 40000)

	var wg sync.WaitGroup
	wg.Go(func() {
		_, writeErr := clientConn.Write(data)
		require.NoError(t, writeErr)
		wf, ok := clientConn.(proto.WriteFlusher)
		assert.True(t, ok, "clientConn must implement WriteFlusher")
		if ok {
			assert.NoError(t, wf.Flush())
		}
	})

	result, err := io.ReadAll(io.LimitReader(serverConn, int64(len(data))))
	require.NoError(t, err)
	assert.Equal(t, data, result)

	wg.Wait()
	clientConn.Close()
	serverConn.Close()
	clientConn.(proto.Releaser).Release()
	serverConn.(proto.Releaser).Release()
}

func TestCompressedConnImplementsWriteFlusher(t *testing.T) {
	t.Parallel()

	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()

	conn, err := proto.NewCompressedConn(clientRaw)
	require.NoError(t, err)
	defer func() {
		conn.Close()
		conn.(proto.Releaser).Release()
	}()

	_, ok := conn.(proto.WriteFlusher)
	assert.True(t, ok, "compressedConn must implement WriteFlusher")
}

func TestNegotiateCompression(t *testing.T) {
	t.Parallel()

	t.Run("both want compression", func(t *testing.T) {
		t.Parallel()
		clientRaw, serverRaw := net.Pipe()

		var (
			clientConn net.Conn
			serverConn net.Conn
			clientErr  error
			serverErr  error
		)

		var wg sync.WaitGroup
		wg.Go(func() {
			clientConn, clientErr = proto.NegotiateCompression(clientRaw, true)
		})
		wg.Go(func() {
			serverConn, serverErr = proto.AcceptCompression(serverRaw)
		})
		wg.Wait()

		require.NoError(t, clientErr)
		require.NoError(t, serverErr)

		// Both should be compressed — verify WriteFlusher
		_, clientOK := clientConn.(proto.WriteFlusher)
		_, serverOK := serverConn.(proto.WriteFlusher)
		assert.True(t, clientOK, "client conn should be compressed")
		assert.True(t, serverOK, "server conn should be compressed")

		clientConn.Close()
		serverConn.Close()
	})

	t.Run("client declines compression", func(t *testing.T) {
		t.Parallel()
		clientRaw, serverRaw := net.Pipe()

		var (
			clientConn net.Conn
			serverConn net.Conn
			clientErr  error
			serverErr  error
		)

		var wg sync.WaitGroup
		wg.Go(func() {
			clientConn, clientErr = proto.NegotiateCompression(clientRaw, false)
		})
		wg.Go(func() {
			serverConn, serverErr = proto.AcceptCompression(serverRaw)
		})
		wg.Wait()

		require.NoError(t, clientErr)
		require.NoError(t, serverErr)

		// Neither should be compressed
		_, clientOK := clientConn.(proto.WriteFlusher)
		_, serverOK := serverConn.(proto.WriteFlusher)
		assert.False(t, clientOK, "client conn should NOT be compressed")
		assert.False(t, serverOK, "server conn should NOT be compressed")

		clientConn.Close()
		serverConn.Close()
	})
}
