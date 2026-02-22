package engine_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/transport/beam"
	"github.com/bamsammich/beam/internal/transport/proto"
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
		//nolint:revive // empty-block: intentionally draining event channel
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

	go func() { _ = daemon.Serve(ctx) }() //nolint:errcheck // test daemon; error not needed

	return daemon.Addr().String(), token
}

// dialBeamReadEndpoint dials a beam daemon and returns a ReadEndpoint.
// The underlying mux is closed on test cleanup.
func dialBeamReadEndpoint(t *testing.T, addr, token, root string) *beam.ReadEndpoint {
	t.Helper()

	mux, _, caps, err := beam.DialBeam(addr, token, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	return beam.NewReadEndpoint(mux, root, caps)
}

// dialBeamWriteEndpoint dials a beam daemon and returns a WriteEndpoint.
// The underlying mux is closed on test cleanup.
func dialBeamWriteEndpoint(t *testing.T, addr, token, root string) *beam.WriteEndpoint {
	t.Helper()

	mux, _, caps, err := beam.DialBeam(addr, token, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	return beam.NewWriteEndpoint(mux, root, caps)
}

// createModifiedTestTree creates a test tree at root that is slightly
// different from createTestTree: root.txt has modified content, big.bin
// has a few changed bytes. Used to test delta transfer.
//
// Modified files are given an mtime 1 hour in the future so that the
// scanner's size+mtime skip detection does not suppress them when the
// source and destination trees are created within the same clock tick.
func createModifiedTestTree(t *testing.T, root string) {
	t.Helper()
	createTestTree(t, root)

	futureTime := time.Now().Add(time.Hour)

	// Modify root.txt (different content AND different size).
	rootPath := filepath.Join(root, "root.txt")
	require.NoError(t, os.WriteFile(rootPath,
		[]byte("MODIFIED root file content -- changed"),
		0o644,
	))
	require.NoError(t, os.Chtimes(rootPath, futureTime, futureTime))

	// Modify a few bytes in big.bin (most blocks unchanged, same size).
	bigPath := filepath.Join(root, "big.bin")
	data, err := os.ReadFile(bigPath)
	require.NoError(t, err)
	copy(data[100:116], []byte("MODIFIED_BLOCK!!"))
	require.NoError(t, os.WriteFile(bigPath, data, 0o644))
	require.NoError(t, os.Chtimes(bigPath, futureTime, futureTime))
}
