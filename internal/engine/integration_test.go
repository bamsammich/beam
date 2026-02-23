package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/engine"
	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
)

func TestIntegration_LocalToLocal(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createTestTree(t, srcDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:   []string{srcDir + "/"},
		Dst:       dstDir,
		Archive:   true,
		Recursive: true,
		Workers:   2,
		Events:    drainEvents(t),
	})

	require.NoError(t, result.Err)
	require.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

	verifyTreeCopy(t, srcDir, dstDir)
}

func TestIntegration_LocalToBeam(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createTestTree(t, srcDir)

	// Start a beam daemon serving dstDir.
	addr, token := startTestDaemon(t, dstDir)
	dstEP := dialBeamWriteEndpoint(t, addr, token, dstDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		DstEndpoint: dstEP,
	})

	require.NoError(t, result.Err)
	require.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

	verifyTreeCopy(t, srcDir, dstDir)
}

// TestIntegration_BeamToLocal_NonLocalPath verifies that beam sources work when
// the source path does not exist on the local filesystem — the path only exists
// on the remote daemon. This is the primary regression test for the bug where
// resolveSources called os.Lstat on remote paths.
//
// Setup: daemon serves parentDir (which contains data/). The endpoint is
// configured with a fictional root ("/nonexistent-beam-test/data") and a
// matching fictional daemonRoot ("/nonexistent-beam-test") so that
// pathPrefix = "data", which maps correctly to parentDir/data/ on the daemon.
func TestIntegration_BeamToLocal_NonLocalPath(t *testing.T) {
	t.Parallel()

	// Guard: skip if our fictional path accidentally exists.
	if _, err := os.Lstat("/nonexistent-beam-test"); err == nil {
		t.Skip("/nonexistent-beam-test exists on this system, skipping")
	}

	parentDir := t.TempDir()
	dataDir := filepath.Join(parentDir, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	createTestTree(t, dataDir)

	dstDir := t.TempDir()

	// Start daemon serving parentDir (data lives at parentDir/data/).
	addr, token := startTestDaemon(t, parentDir)

	// Create a beam ReadEndpoint that simulates a remote source path that
	// does NOT exist locally. pathPrefix = Rel("/nonexistent-beam-test",
	// "/nonexistent-beam-test/data") = "data", which the daemon resolves
	// to parentDir/data/.
	srcEP := dialBeamReadEndpointCustomRoots(t, addr, token,
		"/nonexistent-beam-test/data", "/nonexistent-beam-test")

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{"/nonexistent-beam-test/data/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		SrcEndpoint: srcEP,
	})

	require.NoError(t, result.Err)
	require.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

	verifyTreeCopy(t, dataDir, dstDir)
}

func TestIntegration_BeamToBeam(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createTestTree(t, srcDir)

	// Start beam daemons for both source and destination.
	srcAddr, srcToken := startTestDaemon(t, srcDir)
	dstAddr, dstToken := startTestDaemon(t, dstDir)

	srcEP := dialBeamReadEndpoint(t, srcAddr, srcToken, srcDir)
	dstEP := dialBeamWriteEndpoint(t, dstAddr, dstToken, dstDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		SrcEndpoint: srcEP,
		DstEndpoint: dstEP,
	})

	require.NoError(t, result.Err)
	require.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

	verifyTreeCopy(t, srcDir, dstDir)
}

func TestIntegration_LocalToBeam_Delta(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Pre-populate dst with old version.
	createTestTree(t, dstDir)
	// Create modified source.
	createModifiedTestTree(t, srcDir)

	addr, token := startTestDaemon(t, dstDir)
	dstEP := dialBeamWriteEndpoint(t, addr, token, dstDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		DstEndpoint: dstEP,
		Delta:       true,
	})

	require.NoError(t, result.Err)
	verifyTreeCopy(t, srcDir, dstDir)
}

func TestIntegration_BeamToLocal_Delta(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Source has the new version.
	createModifiedTestTree(t, srcDir)
	// Pre-populate local dst with old version.
	createTestTree(t, dstDir)

	addr, token := startTestDaemon(t, srcDir)
	srcEP := dialBeamReadEndpoint(t, addr, token, srcDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		SrcEndpoint: srcEP,
		Delta:       true,
	})

	require.NoError(t, result.Err)
	verifyTreeCopy(t, srcDir, dstDir)
}

func TestIntegration_BeamToBeam_Delta(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createModifiedTestTree(t, srcDir)
	createTestTree(t, dstDir)

	srcAddr, srcToken := startTestDaemon(t, srcDir)
	dstAddr, dstToken := startTestDaemon(t, dstDir)
	srcEP := dialBeamReadEndpoint(t, srcAddr, srcToken, srcDir)
	dstEP := dialBeamWriteEndpoint(t, dstAddr, dstToken, dstDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		SrcEndpoint: srcEP,
		DstEndpoint: dstEP,
		Delta:       true,
	})

	require.NoError(t, result.Err)
	verifyTreeCopy(t, srcDir, dstDir)
}

func TestIntegration_Delete(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Populate source with standard tree.
	createTestTree(t, srcDir)

	// Pre-populate destination with extraneous files that should be deleted.
	require.NoError(t, os.WriteFile(
		filepath.Join(dstDir, "extra.txt"),
		[]byte("this file should be deleted"),
		0o644,
	))
	require.NoError(t, os.MkdirAll(filepath.Join(dstDir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dstDir, "sub", "orphan.txt"),
		[]byte("this orphan should be deleted"),
		0o644,
	))
	require.NoError(t, os.MkdirAll(filepath.Join(dstDir, "stale"), 0o755))

	result := engine.Run(context.Background(), engine.Config{
		Sources:   []string{srcDir + "/"},
		Dst:       dstDir,
		Archive:   true,
		Recursive: true,
		Workers:   2,
		Delete:    true,
		Events:    drainEvents(t),
	})

	require.NoError(t, result.Err)

	// All source files should be present.
	verifyTreeCopy(t, srcDir, dstDir)

	// Extraneous files/dirs should be gone.
	_, err := os.Stat(filepath.Join(dstDir, "extra.txt"))
	require.True(t, os.IsNotExist(err), "extra.txt should have been deleted")

	_, err = os.Stat(filepath.Join(dstDir, "sub", "orphan.txt"))
	require.True(t, os.IsNotExist(err), "sub/orphan.txt should have been deleted")

	_, err = os.Stat(filepath.Join(dstDir, "stale"))
	require.True(t, os.IsNotExist(err), "stale/ dir should have been deleted")
}

func TestIntegration_InterruptAndResume(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createTestTree(t, srcDir)

	// Phase 1: Interrupt after the first file completes.
	ctx, cancel := context.WithCancel(context.Background())
	evCh := make(chan event.Event, 4096)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range evCh {
			if ev.Type == event.FileCompleted {
				cancel()
			}
		}
	}()

	_ = engine.Run(ctx, engine.Config{
		Sources:   []string{srcDir + "/"},
		Dst:       dstDir,
		Archive:   true,
		Recursive: true,
		Workers:   1, // serialize to make cancellation deterministic
		Events:    evCh,
	})
	close(evCh)
	<-done

	// Verify: no .beam-tmp files left behind (atomic write cleanup).
	tmpFiles := findTmpFiles(t, dstDir)
	require.Empty(t, tmpFiles, "no .beam-tmp files should remain after interrupt")

	// Verify: any files that exist in destination have correct content.
	verifyExistingFilesMatch(t, srcDir, dstDir)

	// Phase 2: Resume — re-run without cancellation.
	result := engine.Run(context.Background(), engine.Config{
		Sources:   []string{srcDir + "/"},
		Dst:       dstDir,
		Archive:   true,
		Recursive: true,
		Workers:   2,
		Events:    drainEvents(t),
	})

	require.NoError(t, result.Err)
	verifyTreeCopy(t, srcDir, dstDir)

	// At least one file should have been skipped (copied in phase 1).
	require.Positive(t, result.Stats.FilesSkipped,
		"resume should skip previously completed files")
}

func TestIntegration_SparseFile(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create sparse file: 4KB data, 1MB hole, 4KB data.
	const dataSize = 4096
	const holeSize = 1024 * 1024 // 1 MB
	sparseFile := filepath.Join(srcDir, "sparse.bin")
	apparentSize := createSparseFile(t, sparseFile, dataSize, holeSize)

	// Also create a small regular file for baseline.
	require.NoError(t, os.WriteFile(
		filepath.Join(srcDir, "normal.txt"),
		[]byte("normal content"),
		0o644,
	))

	result := engine.Run(context.Background(), engine.Config{
		Sources:   []string{srcDir + "/"},
		Dst:       dstDir,
		Archive:   true,
		Recursive: true,
		Workers:   2,
		Events:    drainEvents(t),
	})

	require.NoError(t, result.Err)

	// Verify apparent size matches.
	dstSparse := filepath.Join(dstDir, "sparse.bin")
	dstInfo, err := os.Stat(dstSparse)
	require.NoError(t, err)
	require.Equal(t, apparentSize, dstInfo.Size(), "apparent size should match")

	// Verify content matches byte-for-byte.
	srcData, err := os.ReadFile(sparseFile)
	require.NoError(t, err)
	dstData, err := os.ReadFile(dstSparse)
	require.NoError(t, err)
	require.Equal(t, srcData, dstData, "sparse file content should match")

	// Verify destination is actually sparse (fewer blocks than fully materialized).
	// Skip this check if the filesystem doesn't support sparse files.
	srcStat, err := os.Stat(sparseFile)
	require.NoError(t, err)
	srcBlocks := srcStat.Sys().(*syscall.Stat_t).Blocks
	dstBlocks := dstInfo.Sys().(*syscall.Stat_t).Blocks

	// Only assert sparseness if the SOURCE is actually sparse.
	// tmpfs may materialize the hole, making both files non-sparse.
	fullyMaterializedBlocks := (apparentSize + 511) / 512
	if srcBlocks < fullyMaterializedBlocks/2 {
		// Source is sparse — destination should be too.
		require.Less(t, dstBlocks, fullyMaterializedBlocks/2,
			"destination should preserve sparse structure")
	} else {
		t.Log("filesystem does not support sparse files, skipping sparseness check")
	}

	// Verify normal file copied correctly.
	normalData, err := os.ReadFile(filepath.Join(dstDir, "normal.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("normal content"), normalData)
}

func TestIntegration_Hardlinks(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createHardlinkTree(t, srcDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:   []string{srcDir + "/"},
		Dst:       dstDir,
		Archive:   true,
		Recursive: true,
		Workers:   1, // serialize so the regular copy lands before the hardlink task
		Events:    drainEvents(t),
	})

	require.NoError(t, result.Err)

	// Both files should have the same content as the source.
	srcData, err := os.ReadFile(filepath.Join(srcDir, "original.txt"))
	require.NoError(t, err)

	dstOriginal, err := os.ReadFile(filepath.Join(dstDir, "original.txt"))
	require.NoError(t, err)
	require.Equal(t, srcData, dstOriginal, "content mismatch: original.txt")

	dstHardlink, err := os.ReadFile(filepath.Join(dstDir, "hardlink.txt"))
	require.NoError(t, err)
	require.Equal(t, srcData, dstHardlink, "content mismatch: hardlink.txt")

	// Verify hardlink preserved: same inode.
	origStat, err := os.Stat(filepath.Join(dstDir, "original.txt"))
	require.NoError(t, err)
	linkStat, err := os.Stat(filepath.Join(dstDir, "hardlink.txt"))
	require.NoError(t, err)

	origIno := origStat.Sys().(*syscall.Stat_t).Ino
	linkIno := linkStat.Sys().(*syscall.Stat_t).Ino
	require.Equal(t, origIno, linkIno, "hardlinked files should share the same inode")

	// Verify non-hardlinked file has a different inode.
	anotherStat, err := os.Stat(filepath.Join(dstDir, "sub", "another.txt"))
	require.NoError(t, err)
	anotherIno := anotherStat.Sys().(*syscall.Stat_t).Ino
	require.NotEqual(t, origIno, anotherIno, "non-hardlinked file should have different inode")

	// Stats should reflect hardlink creation.
	require.GreaterOrEqual(t, result.Stats.HardlinksCreated, int64(1),
		"at least one hardlink should be reported")
}

func TestIntegration_Verify(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createTestTree(t, srcDir)

	// Phase 1: Copy with verification — should pass.
	evCh, getEvents := collectEvents(t)
	result := engine.Run(context.Background(), engine.Config{
		Sources:   []string{srcDir + "/"},
		Dst:       dstDir,
		Archive:   true,
		Recursive: true,
		Workers:   2,
		Verify:    true,
		Events:    evCh,
	})

	require.NoError(t, result.Err, "copy with verify should succeed")

	// Check that VerifyOK events were emitted.
	events := getEvents()
	var verifyOKCount int
	for _, ev := range events {
		if ev.Type == event.VerifyOK {
			verifyOKCount++
		}
	}
	require.Positive(t, verifyOKCount, "should have emitted VerifyOK events")

	// Phase 2: Corrupt a destination file and run Verify() directly.
	corruptPath := filepath.Join(dstDir, "root.txt")
	corruptData, err := os.ReadFile(corruptPath)
	require.NoError(t, err)
	corruptData[0] ^= 0xFF // flip bits in first byte
	require.NoError(t, os.WriteFile(corruptPath, corruptData, 0o644))

	srcEP := transport.NewLocalReadEndpoint(srcDir)
	dstEP := transport.NewLocalWriteEndpoint(dstDir)
	defer srcEP.Close()
	defer dstEP.Close()

	vr := engine.Verify(context.Background(), engine.VerifyConfig{
		SrcRoot:     srcDir,
		DstRoot:     dstDir,
		Workers:     2,
		SrcEndpoint: srcEP,
		DstEndpoint: dstEP,
		Stats:       stats.NewCollector(),
	})

	require.Positive(t, vr.Failed, "corrupted file should fail verification")

	// Find the specific failure.
	var foundCorrupt bool
	for _, ve := range vr.Errors {
		if ve.Path == "root.txt" {
			foundCorrupt = true
			break
		}
	}
	require.True(t, foundCorrupt, "root.txt should appear in verification errors")
}

// testTreeByteSize is the exact sum of regular file bytes in createTestTree:
//
//	root.txt       = 17
//	big.bin        = 320000  (16 × 20000)
//	sub/mid.txt    = 19
//	sub/deep/leaf.txt = 17
//	link.txt       = symlink (0 copied bytes)
const testTreeByteSize = 17 + 320000 + 19 + 17 // 320053

// TestIntegration_ByteCount_LocalToLocal verifies that Stats.BytesCopied
// exactly matches the total regular file bytes for a local-to-local copy.
func TestIntegration_ByteCount_LocalToLocal(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	createTestTree(t, srcDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:   []string{srcDir + "/"},
		Dst:       dstDir,
		Archive:   true,
		Recursive: true,
		Workers:   2,
		Events:    drainEvents(t),
	})

	require.NoError(t, result.Err)
	require.Equal(t, int64(testTreeByteSize), result.Stats.BytesCopied,
		"BytesCopied must exactly match source file sizes (local→local)")
}

// TestIntegration_ByteCount_BeamToLocal verifies that Stats.BytesCopied
// exactly matches the total regular file bytes when copying from a beam
// source to a local destination. This is the path where progressWriter
// reports bytes incrementally — no double-counting allowed.
func TestIntegration_ByteCount_BeamToLocal(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	createTestTree(t, srcDir)

	addr, token := startTestDaemon(t, srcDir)
	srcEP := dialBeamReadEndpoint(t, addr, token, srcDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		SrcEndpoint: srcEP,
	})

	require.NoError(t, result.Err)
	require.Equal(t, int64(testTreeByteSize), result.Stats.BytesCopied,
		"BytesCopied must exactly match source file sizes (beam→local, progressWriter path)")
	verifyTreeCopy(t, srcDir, dstDir)
}

// TestIntegration_ByteCount_LocalToBeam verifies that Stats.BytesCopied
// exactly matches the total regular file bytes when copying from a local
// source to a beam destination.
func TestIntegration_ByteCount_LocalToBeam(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	createTestTree(t, srcDir)

	addr, token := startTestDaemon(t, dstDir)
	dstEP := dialBeamWriteEndpoint(t, addr, token, dstDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		DstEndpoint: dstEP,
	})

	require.NoError(t, result.Err)
	require.Equal(t, int64(testTreeByteSize), result.Stats.BytesCopied,
		"BytesCopied must exactly match source file sizes (local→beam)")
	verifyTreeCopy(t, srcDir, dstDir)
}

// TestIntegration_ByteCount_BeamToBeam verifies that Stats.BytesCopied
// exactly matches the total regular file bytes for a beam-to-beam copy.
func TestIntegration_ByteCount_BeamToBeam(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	createTestTree(t, srcDir)

	srcAddr, srcToken := startTestDaemon(t, srcDir)
	dstAddr, dstToken := startTestDaemon(t, dstDir)
	srcEP := dialBeamReadEndpoint(t, srcAddr, srcToken, srcDir)
	dstEP := dialBeamWriteEndpoint(t, dstAddr, dstToken, dstDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		SrcEndpoint: srcEP,
		DstEndpoint: dstEP,
	})

	require.NoError(t, result.Err)
	require.Equal(t, int64(testTreeByteSize), result.Stats.BytesCopied,
		"BytesCopied must exactly match source file sizes (beam→beam)")
	verifyTreeCopy(t, srcDir, dstDir)
}

// TestIntegration_BeamToLocal_PathEscapePrevented verifies that a beam source
// with a URL path outside the daemon root does NOT cause the daemon to walk
// outside its root. The computePathPrefix function must return "" when
// filepath.Rel(daemonRoot, root) would produce ".." components.
//
// Scenario: daemon serves /tmp/X, user connects with loc.Path="/".
// Without the fix, pathPrefix = "../../.." which escapes the daemon root.
// With the fix, pathPrefix = "" which maps to the daemon root.
func TestIntegration_BeamToLocal_PathEscapePrevented(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	createTestTree(t, srcDir)

	// Start daemon serving srcDir (a specific subdirectory).
	addr, token := startTestDaemon(t, srcDir)

	// Simulate a client connecting with loc.Path="/" while daemon root is srcDir.
	// Without the fix, computePathPrefix("/", srcDir) would produce "../../../..."
	// which escapes the daemon root. With the fix, it produces "" (daemon root).
	srcEP := dialBeamReadEndpointCustomRoots(t, addr, token, "/", srcDir)

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{"/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		SrcEndpoint: srcEP,
	})

	require.NoError(t, result.Err)

	// Should have copied exactly the daemon root's content (the test tree),
	// NOT the entire filesystem.
	require.Equal(t, int64(testTreeByteSize), result.Stats.BytesCopied,
		"should only copy daemon root content, not escape to parent directories")
	verifyTreeCopy(t, srcDir, dstDir)
}
