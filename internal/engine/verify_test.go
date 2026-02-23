package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
)

func TestVerify_MatchingFiles(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(dst, "sub"), 0755))

	// Write identical files.
	for _, relPath := range []string{"a.txt", "sub/b.txt"} {
		data := []byte("content of " + relPath)
		require.NoError(t, os.WriteFile(filepath.Join(src, relPath), data, 0644))
		require.NoError(t, os.WriteFile(filepath.Join(dst, relPath), data, 0644))
	}

	collector := stats.NewCollector()
	events := make(chan event.Event, 64)

	vr := Verify(context.Background(), VerifyConfig{
		SrcRoot:     src,
		DstRoot:     dst,
		Workers:     2,
		Stats:       collector,
		Events:      events,
		SrcEndpoint: transport.NewLocalReadEndpoint(src),
		DstEndpoint: transport.NewLocalWriteEndpoint(dst),
	})
	close(events)

	assert.Equal(t, int64(2), vr.Verified)
	assert.Equal(t, int64(0), vr.Failed)
	assert.Empty(t, vr.Errors)
	assert.Equal(t, int64(2), collector.Snapshot().FilesVerified)
}

func TestVerify_CorruptedFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))

	require.NoError(t, os.WriteFile(filepath.Join(src, "file.txt"), []byte("correct"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "file.txt"), []byte("corrupted"), 0644))

	collector := stats.NewCollector()
	events := make(chan event.Event, 64)

	vr := Verify(context.Background(), VerifyConfig{
		SrcRoot:     src,
		DstRoot:     dst,
		Workers:     1,
		Stats:       collector,
		Events:      events,
		SrcEndpoint: transport.NewLocalReadEndpoint(src),
		DstEndpoint: transport.NewLocalWriteEndpoint(dst),
	})
	close(events)

	assert.Equal(t, int64(0), vr.Verified)
	assert.Equal(t, int64(1), vr.Failed)
	assert.Len(t, vr.Errors, 1)
	assert.Equal(t, "file.txt", vr.Errors[0].Path)
	assert.NotEqual(t, vr.Errors[0].SrcHash, vr.Errors[0].DstHash)
	assert.Equal(t, int64(1), collector.Snapshot().FilesVerifyFailed)
}

func TestVerify_FilterRespected(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))

	// Write matching file that should be verified.
	require.NoError(t, os.WriteFile(filepath.Join(src, "keep.txt"), []byte("data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "keep.txt"), []byte("data"), 0644))

	// Write file that should be excluded — with different content to detect leak.
	require.NoError(t, os.WriteFile(filepath.Join(src, "skip.log"), []byte("src"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "skip.log"), []byte("dst"), 0644))

	chain := filter.NewChain()
	require.NoError(t, chain.AddExclude("*.log"))

	collector := stats.NewCollector()
	events := make(chan event.Event, 64)

	vr := Verify(context.Background(), VerifyConfig{
		SrcRoot:     src,
		DstRoot:     dst,
		Workers:     1,
		Filter:      chain,
		Stats:       collector,
		Events:      events,
		SrcEndpoint: transport.NewLocalReadEndpoint(src),
		DstEndpoint: transport.NewLocalWriteEndpoint(dst),
	})
	close(events)

	assert.Equal(t, int64(1), vr.Verified)
	assert.Equal(t, int64(0), vr.Failed) // excluded file not verified
}

func TestVerify_Events(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.MkdirAll(dst, 0755))

	require.NoError(t, os.WriteFile(filepath.Join(src, "ok.txt"), []byte("same"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "ok.txt"), []byte("same"), 0644))

	require.NoError(t, os.WriteFile(filepath.Join(src, "bad.txt"), []byte("a"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "bad.txt"), []byte("b"), 0644))

	collector := stats.NewCollector()
	events := make(chan event.Event, 64)
	var collected []event.Event
	done := make(chan struct{})
	go func() {
		for ev := range events {
			collected = append(collected, ev)
		}
		close(done)
	}()

	Verify(context.Background(), VerifyConfig{
		SrcRoot:     src,
		DstRoot:     dst,
		Workers:     1,
		Stats:       collector,
		Events:      events,
		SrcEndpoint: transport.NewLocalReadEndpoint(src),
		DstEndpoint: transport.NewLocalWriteEndpoint(dst),
	})
	close(events)
	<-done

	typeSet := make(map[event.Type]bool)
	for _, ev := range collected {
		typeSet[ev.Type] = true
	}

	assert.True(t, typeSet[event.VerifyStarted])
	assert.True(t, typeSet[event.VerifyOK])
	assert.True(t, typeSet[event.VerifyFailed])
}

func TestEngine_CopyWithVerify(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "file.txt"), []byte("data"), 0644))

	srcConn, dstConn := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src},
		Dst:          dst,
		Recursive:    true,
		Workers:      2,
		Verify:       true,
		SrcConnector: srcConn,
		DstConnector: dstConn,
	})

	require.NoError(t, result.Err)
	assert.Equal(t, int64(1), result.Stats.FilesVerified)
	assert.Equal(t, int64(0), result.Stats.FilesVerifyFailed)
}

func TestEngine_VerifySkippedOnDryRun(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "file.txt"), []byte("data"), 0644))

	srcConn2, dstConn2 := localConns()
	result := Run(context.Background(), Config{
		Sources:      []string{src},
		Dst:          dst,
		Recursive:    true,
		Workers:      1,
		Verify:       true,
		DryRun:       true,
		SrcConnector: srcConn2,
		DstConnector: dstConn2,
	})

	require.NoError(t, result.Err)
	assert.Equal(t, int64(0), result.Stats.FilesVerified)
}
