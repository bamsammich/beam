package engine_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/transport"
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

// generateTestAuthOpts creates a fresh SSH key pair and returns auth opts.
func generateTestAuthOpts(t *testing.T) proto.AuthOpts {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	require.NoError(t, err)
	return proto.AuthOpts{
		Username: "testuser",
		Signer:   signer,
	}
}

// startTestDaemon starts a beam daemon serving root on a random port.
// Returns the address and auth opts. The daemon is stopped on test cleanup.
func startTestDaemon(t *testing.T, root string) (addr string, authOpts proto.AuthOpts) {
	t.Helper()

	authOpts = generateTestAuthOpts(t)
	daemon, err := proto.NewDaemon(proto.DaemonConfig{
		ListenAddr: "127.0.0.1:0",
		Root:       root,
		KeyChecker: func(_ string, _ ssh.PublicKey) bool { return true },
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = daemon.Serve(ctx) }() //nolint:errcheck // test daemon; error not needed

	return daemon.Addr().String(), authOpts
}

// dialBeamTransport dials a beam daemon and returns a Transport.
// The underlying mux is closed on test cleanup.
//
//nolint:ireturn // test helper returns Transport
func dialBeamTransport(
	t *testing.T, addr string, authOpts proto.AuthOpts, root string,
) transport.Transport {
	t.Helper()

	mux, _, caps, err := beam.DialBeam(addr, authOpts, proto.ClientTLSConfig(), true)
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	return beam.NewTransportFromMux(mux, root, caps)
}

// dialBeamTransportCustomRoots dials a beam daemon and returns a Transport
// with explicit daemonRoot. This allows tests to simulate non-local source
// paths by constructing a pathPrefix that maps a fictional client-side root
// to real data on the daemon.
//
//nolint:ireturn // test helper returns Transport
func dialBeamTransportCustomRoots(
	t *testing.T,
	addr string, authOpts proto.AuthOpts, daemonRoot string,
) transport.Transport {
	t.Helper()

	mux, _, caps, err := beam.DialBeam(addr, authOpts, proto.ClientTLSConfig(), true)
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	return beam.NewTransportFromMux(mux, daemonRoot, caps)
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
// events. The getter closes the channel and waits for the drain goroutine,
// so it is safe to read the slice. It may be called at most once. If the
// getter is never called, t.Cleanup closes the channel on test exit.
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
	var once sync.Once
	drain := func() {
		once.Do(func() { close(ch) })
		<-done
	}
	t.Cleanup(drain)
	return ch, func() []event.Event {
		drain()
		return collected
	}
}

// findTmpFiles returns any .beam-tmp files found under root.
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
func verifyExistingFilesMatch(t *testing.T, srcRoot, dstRoot string) {
	t.Helper()
	err := filepath.WalkDir(dstRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Type()&os.ModeSymlink != 0 {
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
