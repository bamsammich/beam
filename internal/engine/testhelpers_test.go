package engine_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/beam"
	"github.com/bamsammich/beam/internal/transport/proto"
	"github.com/stretchr/testify/require"
)

// createTestTree populates root with a standard test tree:
//
//	root.txt          (17 bytes)
//	big.bin           (320KB)
//	sub/mid.txt       (19 bytes)
//	sub/deep/leaf.txt (17 bytes)
//	link.txt          → root.txt (symlink)
func createTestTree(t *testing.T, root string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub", "deep"), 0o755))

	require.NoError(t, os.WriteFile(
		filepath.Join(root, "root.txt"),
		[]byte("root file content"),
		0o644,
	))

	bigData := bytes.Repeat([]byte("ABCDEFGHIJKLMNOP"), 20000) // 320KB
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "big.bin"),
		bigData,
		0o644,
	))

	require.NoError(t, os.WriteFile(
		filepath.Join(root, "sub", "mid.txt"),
		[]byte("middle file content"),
		0o644,
	))

	require.NoError(t, os.WriteFile(
		filepath.Join(root, "sub", "deep", "leaf.txt"),
		[]byte("leaf file content"),
		0o644,
	))

	require.NoError(t, os.Symlink("root.txt", filepath.Join(root, "link.txt")))
}

// verifyTreeCopy checks that dstRoot contains an exact copy of the test tree
// created by createTestTree under srcRoot.
func verifyTreeCopy(t *testing.T, srcRoot, dstRoot string) {
	t.Helper()

	// Regular files: byte-for-byte content match.
	files := []string{
		"root.txt",
		"big.bin",
		filepath.Join("sub", "mid.txt"),
		filepath.Join("sub", "deep", "leaf.txt"),
	}
	for _, rel := range files {
		srcData, err := os.ReadFile(filepath.Join(srcRoot, rel))
		require.NoError(t, err, "read src %s", rel)

		dstData, err := os.ReadFile(filepath.Join(dstRoot, rel))
		require.NoError(t, err, "read dst %s", rel)

		require.Equal(t, srcData, dstData, "content mismatch: %s", rel)
	}

	// Directories exist.
	for _, dir := range []string{"sub", filepath.Join("sub", "deep")} {
		info, err := os.Stat(filepath.Join(dstRoot, dir))
		require.NoError(t, err, "stat dir %s", dir)
		require.True(t, info.IsDir(), "%s should be a directory", dir)
	}

	// Symlink target preserved.
	target, err := os.Readlink(filepath.Join(dstRoot, "link.txt"))
	require.NoError(t, err, "readlink link.txt")
	require.Equal(t, "root.txt", target)
}

// drainEvents creates a buffered event channel, spawns a goroutine to drain
// it, and registers cleanup. Returns the channel for use in engine.Config.
func drainEvents(t *testing.T) chan<- event.Event {
	t.Helper()
	ch := make(chan event.Event, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	t.Cleanup(func() {
		close(ch)
		<-done
	})
	return ch
}

// startTestDaemon starts a beam daemon serving root on a random port.
// Returns the address and auth token. The daemon is stopped on test cleanup.
func startTestDaemon(t *testing.T, root string) (addr, token string) {
	t.Helper()

	token = "integration-test-token"
	daemon, err := proto.NewDaemon(proto.DaemonConfig{
		ListenAddr: "127.0.0.1:0",
		Root:       root,
		AuthToken:  token,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go daemon.Serve(ctx)

	return daemon.Addr().String(), token
}

// dialBeamReadEndpoint dials a beam daemon and returns a BeamReadEndpoint.
// The underlying mux is closed on test cleanup.
func dialBeamReadEndpoint(t *testing.T, addr, token, root string) *beam.BeamReadEndpoint {
	t.Helper()

	mux, _, caps, err := beam.DialBeam(addr, token, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	return beam.NewBeamReadEndpoint(mux, root, caps)
}

// dialBeamWriteEndpoint dials a beam daemon and returns a BeamWriteEndpoint.
// The underlying mux is closed on test cleanup.
func dialBeamWriteEndpoint(t *testing.T, addr, token, root string) *beam.BeamWriteEndpoint {
	t.Helper()

	mux, _, caps, err := beam.DialBeam(addr, token, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	return beam.NewBeamWriteEndpoint(mux, root, caps)
}

// reRootedReadEndpoint wraps a ReadEndpoint to override Root() while
// delegating all I/O to the underlying endpoint. This is needed for
// sftp-to-local tests where the scanner walks the local filesystem but
// the worker computes relSrc via SrcEndpoint.Root().
type reRootedReadEndpoint struct {
	transport.ReadEndpoint
	localRoot string
}

func (e *reRootedReadEndpoint) Root() string { return e.localRoot }
