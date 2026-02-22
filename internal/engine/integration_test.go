package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/engine"
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
