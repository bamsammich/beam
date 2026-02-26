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
}

func TestCompressedConnImplementsWriteFlusher(t *testing.T) {
	t.Parallel()

	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()

	conn, err := proto.NewCompressedConn(clientRaw)
	require.NoError(t, err)
	defer conn.Close()

	_, ok := conn.(proto.WriteFlusher)
	assert.True(t, ok, "compressedConn must implement WriteFlusher")
}
