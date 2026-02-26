package beam_test

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/beam"
	"github.com/bamsammich/beam/internal/transport/proto"
)

// TestDialBeamConn verifies the beam handshake over a raw net.Pipe connection
// (no TLS). The server side simulates the pubkey auth flow, the client side
// calls DialBeamConn.
func TestDialBeamConn(t *testing.T) {
	t.Parallel()

	// Create a temp directory with test content.
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello"), 0o644))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "sub", "nested.txt"), []byte("nested"), 0o644,
	))

	authOpts := generateTestAuthOpts(t)

	// Create a net.Pipe — bidirectional in-memory connection.
	clientConn, serverConn := net.Pipe()

	// Start the server side in a goroutine: accept compression, create mux,
	// handler, and auth listener. Must be concurrent because net.Pipe
	// requires both sides to be running for the compression handshake.
	readEP := transport.NewLocalReader(dir)
	writeEP := transport.NewLocalWriter(dir)

	go func() {
		muxConn, accErr := proto.AcceptCompression(serverConn)
		if accErr != nil {
			serverConn.Close()
			return
		}
		serverMux := proto.NewMux(muxConn)
		handler := proto.NewHandler(readEP, writeEP, serverMux)

		controlCh := serverMux.OpenStream(proto.ControlStream)
		serverMux.SetHandler(func(streamID uint32, ch <-chan proto.Frame) {
			handler.ServeStream(streamID, ch)
			serverMux.CloseStream(streamID)
		})

		go serverMux.Run() //nolint:errcheck // test server; error not needed

		defer serverMux.Close()

		// Use ServerAuth with a key checker that accepts the test key.
		sa := proto.NewServerAuth(dir)
		sa.KeyChecker = func(_ string, _ ssh.PublicKey) bool { return true }

		username, err := sa.Authenticate(serverMux, controlCh)
		if err != nil {
			return
		}
		_ = username

		// Keep the server alive until mux closes.
		<-serverMux.Done()
	}()

	// Client side: DialBeamConn over the pipe.
	mux, root, caps, err := beam.DialBeamConn(clientConn, authOpts, false)
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	assert.Equal(t, dir, root)
	assert.True(t, caps.NativeHash)
	assert.True(t, caps.AtomicRename)

	// Create a Reader and verify it works.
	readBeamEP := beam.NewReader(mux, dir, root, caps)

	// Test Stat.
	entry, err := readBeamEP.Stat("hello.txt")
	require.NoError(t, err)
	assert.Equal(t, "hello.txt", entry.RelPath)
	assert.Equal(t, int64(5), entry.Size)

	// Test Walk.
	var paths []string
	err = readBeamEP.Walk(func(entry transport.FileEntry) error {
		paths = append(paths, entry.RelPath)
		return nil
	})
	require.NoError(t, err)
	assert.Contains(t, paths, "hello.txt")
	assert.Contains(t, paths, filepath.Join("sub", "nested.txt"))

	// Test Hash.
	hash, err := readBeamEP.Hash("hello.txt")
	require.NoError(t, err)
	assert.Len(t, hash, 64) // BLAKE3 hex

	// Test OpenRead.
	rc, err := readBeamEP.OpenRead("hello.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, "hello", string(data))
}
