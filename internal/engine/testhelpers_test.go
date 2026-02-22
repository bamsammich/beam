package engine_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
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

// createHardlinkTree populates root with files that include hardlinks:
//
//	original.txt   (21 bytes)
//	hardlink.txt   → hardlink to original.txt
//	sub/another.txt (23 bytes)
//
//nolint:unused // used by TestIntegration_Hardlinks added in a subsequent commit
func createHardlinkTree(t *testing.T, root string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub"), 0o755))

	require.NoError(t, os.WriteFile(
		filepath.Join(root, "original.txt"),
		[]byte("original file content"),
		0o644,
	))

	require.NoError(t, os.Link(
		filepath.Join(root, "original.txt"),
		filepath.Join(root, "hardlink.txt"),
	))

	require.NoError(t, os.WriteFile(
		filepath.Join(root, "sub", "another.txt"),
		[]byte("another file, different"),
		0o644,
	))
}

// createSparseFile creates a file at path with two data regions separated by a
// hole. Layout: [dataSize bytes of 'A'] [holeSize gap] [dataSize bytes of 'B'].
// Returns the apparent file size (2*dataSize + holeSize).
//
// If the filesystem does not support sparse files (e.g. tmpfs), the file is
// still created but without holes — callers should handle this.
//
//nolint:unused // used by TestIntegration_SparseFile added in a subsequent commit
func createSparseFile(t *testing.T, path string, dataSize, holeSize int64) int64 {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	// First data region.
	_, err = f.Write(bytes.Repeat([]byte("A"), int(dataSize)))
	require.NoError(t, err)

	// Seek past the hole.
	_, err = f.Seek(dataSize+holeSize, 0)
	require.NoError(t, err)

	// Second data region.
	_, err = f.Write(bytes.Repeat([]byte("B"), int(dataSize)))
	require.NoError(t, err)

	return 2*dataSize + holeSize
}

// collectEvents creates a buffered event channel that records all events.
// Returns the channel for engine.Config and a function to retrieve collected
// events (safe to call only after the engine has finished and the channel is
// closed via cleanup).
//
//nolint:unused // used by scenario integration tests added in subsequent commits
func collectEvents(t *testing.T) (chan<- event.Event, func() []event.Event) {
	t.Helper()
	ch := make(chan event.Event, 4096)
	var collected []event.Event
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range ch {
			collected = append(collected, ev)
		}
	}()
	t.Cleanup(func() {
		close(ch)
		<-done
	})
	return ch, func() []event.Event {
		return collected
	}
}

// findTmpFiles returns any .beam-tmp files found under root.
//
//nolint:unused // used by TestIntegration_InterruptAndResume added in a subsequent commit
func findTmpFiles(t *testing.T, root string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(d.Name(), ".beam-tmp") {
			found = append(found, path)
		}
		return nil
	})
	require.NoError(t, err)
	return found
}

// verifyExistingFilesMatch checks that every regular file in dstRoot that also
// exists in srcRoot has identical content. Files only in dstRoot are ignored.
//
//nolint:unused // used by TestIntegration_InterruptAndResume added in a subsequent commit
func verifyExistingFilesMatch(t *testing.T, srcRoot, dstRoot string) {
	t.Helper()
	err := filepath.WalkDir(dstRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dstRoot, path)
		require.NoError(t, err)

		srcPath := filepath.Join(srcRoot, rel)
		if _, statErr := os.Stat(srcPath); os.IsNotExist(statErr) {
			return nil // file only in dst, skip
		}

		srcData, err := os.ReadFile(srcPath)
		require.NoError(t, err, "read src %s", rel)
		dstData, err := os.ReadFile(path)
		require.NoError(t, err, "read dst %s", rel)
		require.Equal(t, srcData, dstData, "content mismatch (interrupted copy?): %s", rel)
		return nil
	})
	require.NoError(t, err)
}
