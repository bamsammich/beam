package beam_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/engine"
	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/beam"
	"github.com/bamsammich/beam/internal/transport/proto"
)

// TestBeamToBeamTransfer exercises a full directory copy through the beam
// protocol: source daemon → ReadEndpoint → WriteEndpoint → dest daemon.
// This mimics what the engine does for a beam-to-beam transfer.
func TestBeamToBeamTransfer(t *testing.T) {
	t.Parallel()

	// --- Set up source tree ---
	srcDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "sub", "deep"), 0o755))
	require.NoError(
		t,
		os.WriteFile(filepath.Join(srcDir, "root.txt"), []byte("root file content"), 0o644),
	)
	require.NoError(
		t,
		os.WriteFile(filepath.Join(srcDir, "sub", "mid.txt"), []byte("middle file"), 0o644),
	)
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(srcDir, "sub", "deep", "leaf.txt"),
			[]byte("leaf content here"),
			0o644,
		),
	)
	// Large-ish file to test chunked transfer (bigger than DataChunkSize=256KB).
	bigData := bytes.Repeat([]byte("ABCDEFGHIJKLMNOP"), 20000) // 320KB
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "big.bin"), bigData, 0o644))
	// Symlink.
	require.NoError(t, os.Symlink("root.txt", filepath.Join(srcDir, "link.txt")))

	dstDir := t.TempDir()

	// --- Start source and dest daemons ---
	srcAddr, srcToken := startTestDaemon(t, srcDir)
	dstAddr, dstToken := startTestDaemon(t, dstDir)

	// --- Connect to both ---
	srcMux, _, srcCaps, err := beam.DialBeam(srcAddr, srcToken, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { srcMux.Close() })

	dstMux, _, dstCaps, err := beam.DialBeam(dstAddr, dstToken, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { dstMux.Close() })

	srcEP := beam.NewReadEndpoint(srcMux, srcDir, srcDir, srcCaps)
	dstEP := beam.NewWriteEndpoint(dstMux, dstDir, dstDir, dstCaps)

	// --- Walk source, replicate to dest ---
	var entries []transport.FileEntry
	err = srcEP.Walk(func(entry transport.FileEntry) error {
		entries = append(entries, entry)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	// Sort entries so directories come before their contents.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].RelPath < entries[j].RelPath
	})

	for _, entry := range entries {
		if entry.IsDir {
			require.NoError(t, dstEP.MkdirAll(entry.RelPath, entry.Mode.Perm()))
			continue
		}
		if entry.IsSymlink {
			require.NoError(t, dstEP.Symlink(entry.LinkTarget, entry.RelPath))
			continue
		}

		// Regular file: open, stream, atomic write.
		rc, openErr := srcEP.OpenRead(entry.RelPath)
		require.NoError(t, openErr)

		wf, createErr := dstEP.CreateTemp(entry.RelPath, entry.Mode.Perm())
		require.NoError(t, createErr)

		_, copyErr := io.Copy(wf, rc)
		require.NoError(t, copyErr)
		require.NoError(t, rc.Close())
		require.NoError(t, wf.Close())
		require.NoError(t, dstEP.Rename(wf.Name(), entry.RelPath))
	}

	// --- Verify destination matches source ---

	// 1. Regular files: content matches.
	for _, name := range []string{"root.txt", "sub/mid.txt", "sub/deep/leaf.txt", "big.bin"} {
		srcContent, readSrcErr := os.ReadFile(filepath.Join(srcDir, name))
		require.NoError(t, readSrcErr, "read source %s", name)
		dstContent, readDstErr := os.ReadFile(filepath.Join(dstDir, name))
		require.NoError(t, readDstErr, "read dest %s", name)
		assert.Equal(t, srcContent, dstContent, "content mismatch for %s", name)
	}

	// 2. Directory structure exists.
	info, err := os.Stat(filepath.Join(dstDir, "sub", "deep"))
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// 3. Symlink reproduced.
	linkTarget, err := os.Readlink(filepath.Join(dstDir, "link.txt"))
	require.NoError(t, err)
	assert.Equal(t, "root.txt", linkTarget)

	// 4. Server-side hash verification: compare BLAKE3 hashes via protocol.
	for _, name := range []string{"root.txt", "big.bin"} {
		srcHash, err := srcEP.Hash(name)
		require.NoError(t, err)
		dstHash, err := dstEP.Hash(name)
		require.NoError(t, err)
		assert.Equal(t, srcHash, dstHash, "hash mismatch for %s", name)
	}
}

// TestBeamToBeamDeleteSync tests that after a beam transfer, we can detect
// and remove extraneous files on the destination.
func TestBeamToBeamDeleteSync(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "keep.txt"), []byte("keep"), 0o644))

	dstDir := t.TempDir()
	// Pre-populate dest with a file not in source.
	require.NoError(t, os.WriteFile(filepath.Join(dstDir, "keep.txt"), []byte("keep"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dstDir, "extra.txt"), []byte("extra"), 0o644))

	dstAddr, dstToken := startTestDaemon(t, dstDir)

	dstMux, _, dstCaps, err := beam.DialBeam(dstAddr, dstToken, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { dstMux.Close() })

	dstEP := beam.NewWriteEndpoint(dstMux, dstDir, dstDir, dstCaps)

	// Walk destination to find extraneous files.
	srcFiles := map[string]bool{"keep.txt": true}
	var toDelete []string

	err = dstEP.Walk(func(entry transport.FileEntry) error {
		if !entry.IsDir && !srcFiles[entry.RelPath] {
			toDelete = append(toDelete, entry.RelPath)
		}
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"extra.txt"}, toDelete)

	// Delete extraneous files via protocol.
	for _, rel := range toDelete {
		require.NoError(t, dstEP.Remove(rel))
	}

	// Verify extra.txt is gone.
	_, err = os.Stat(filepath.Join(dstDir, "extra.txt"))
	assert.True(t, os.IsNotExist(err))

	// Verify keep.txt is still there.
	_, err = os.Stat(filepath.Join(dstDir, "keep.txt"))
	assert.NoError(t, err)
}

// TestBeamToBeamMetadata verifies that file metadata can be set via beam protocol.
func TestBeamToBeamMetadata(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "meta.txt"), []byte("data"), 0o600))

	dstDir := t.TempDir()

	srcAddr, srcToken := startTestDaemon(t, srcDir)
	dstAddr, dstToken := startTestDaemon(t, dstDir)

	srcMux, _, srcCaps, err := beam.DialBeam(srcAddr, srcToken, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { srcMux.Close() })

	dstMux, _, dstCaps, err := beam.DialBeam(dstAddr, dstToken, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { dstMux.Close() })

	srcEP := beam.NewReadEndpoint(srcMux, srcDir, srcDir, srcCaps)
	dstEP := beam.NewWriteEndpoint(dstMux, dstDir, dstDir, dstCaps)

	// Copy the file.
	entry, err := srcEP.Stat("meta.txt")
	require.NoError(t, err)

	rc, err := srcEP.OpenRead("meta.txt")
	require.NoError(t, err)
	wf, err := dstEP.CreateTemp("meta.txt", 0o644)
	require.NoError(t, err)
	_, err = io.Copy(wf, rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.NoError(t, wf.Close())
	require.NoError(t, dstEP.Rename(wf.Name(), "meta.txt"))

	// Set metadata (permissions, mtime).
	require.NoError(t, dstEP.SetMetadata("meta.txt", entry, transport.MetadataOpts{
		Mode:  true,
		Times: true,
	}))

	// Verify permissions.
	info, err := os.Stat(filepath.Join(dstDir, "meta.txt"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	// Verify mtime matches (within 1 second due to filesystem precision).
	assert.InDelta(t, entry.ModTime.UnixNano(), info.ModTime().UnixNano(), float64(1e9))
}

// TestBeamEndToEndEngineIntegration uses the actual engine to perform a copy
// with a WriteEndpoint as destination. This is the closest we can get to
// what `beam -a /src beam://token@host/dst` does.
func TestBeamEndToEndEngineIntegration(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "dir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "dir", "a.txt"), []byte("file a"), 0o644))
	require.NoError(
		t,
		os.WriteFile(filepath.Join(srcDir, "dir", "b.txt"), []byte("file b content"), 0o644),
	)

	dstDir := t.TempDir()

	dstAddr, dstToken := startTestDaemon(t, dstDir)

	dstMux, _, dstCaps, err := beam.DialBeam(dstAddr, dstToken, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { dstMux.Close() })

	dstEP := beam.NewWriteEndpoint(dstMux, dstDir, dstDir, dstCaps)

	// Use the engine to copy srcDir/dir/ → dstDir/ via beam endpoint.
	ctx := context.Background()
	events := make(chan event.Event, 256)

	// Drain events in background.
	go func() {
		//nolint:revive // empty-block: intentionally draining event channel
		for range events {
		}
	}()

	result := engine.Run(ctx, engine.Config{
		Sources:     []string{srcDir + "/dir/"},
		Dst:         dstDir,
		Recursive:   true,
		Archive:     true,
		Workers:     2,
		Events:      events,
		DstEndpoint: dstEP,
	})
	close(events)

	require.NoError(t, result.Err)
	assert.Equal(t, int64(2), result.Stats.FilesCopied)

	// Verify files landed in dstDir (not dstDir/dir, because trailing slash).
	contentA, err := os.ReadFile(filepath.Join(dstDir, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "file a", string(contentA))

	contentB, err := os.ReadFile(filepath.Join(dstDir, "b.txt"))
	require.NoError(t, err)
	assert.Equal(t, "file b content", string(contentB))
}
