package engine_test

import (
	"context"
	"testing"

	"github.com/bamsammich/beam/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

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
	assert.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

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
	assert.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

	verifyTreeCopy(t, srcDir, dstDir)
}
