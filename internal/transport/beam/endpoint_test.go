package beam_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/beam"
	"github.com/bamsammich/beam/internal/transport/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startTestDaemon starts a daemon on a random port and returns connect info.
func startTestDaemon(t *testing.T, root string) (addr, token string) {
	t.Helper()

	token = "test-token-123"
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

func dialTestEndpoints(t *testing.T, root string) (*beam.BeamReadEndpoint, *beam.BeamWriteEndpoint) {
	t.Helper()

	addr, token := startTestDaemon(t, root)

	mux, _, caps, err := beam.DialBeam(addr, token, proto.ClientTLSConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	readEP := beam.NewBeamReadEndpoint(mux, root, caps)
	writeEP := beam.NewBeamWriteEndpoint(mux, root, caps)
	return readEP, writeEP
}

func TestBeamReadEndpointStat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello world"), 0o644))

	readEP, _ := dialTestEndpoints(t, dir)

	entry, err := readEP.Stat("hello.txt")
	require.NoError(t, err)
	assert.Equal(t, "hello.txt", entry.RelPath)
	assert.Equal(t, int64(11), entry.Size)
	assert.False(t, entry.IsDir)
}

func TestBeamReadEndpointWalk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sub", "b.txt"), []byte("bb"), 0o644))

	readEP, _ := dialTestEndpoints(t, dir)

	var paths []string
	err := readEP.Walk(func(entry transport.FileEntry) error {
		paths = append(paths, entry.RelPath)
		return nil
	})
	require.NoError(t, err)

	sort.Strings(paths)
	assert.Contains(t, paths, "a.txt")
	assert.Contains(t, paths, filepath.Join("sub", "b.txt"))
}

func TestBeamReadEndpointReadDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "one.txt"), []byte("1"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "two.txt"), []byte("22"), 0o644))

	readEP, _ := dialTestEndpoints(t, dir)

	entries, err := readEP.ReadDir(".")
	require.NoError(t, err)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.RelPath
	}
	sort.Strings(names)
	assert.Contains(t, names, "one.txt")
	assert.Contains(t, names, "two.txt")
	assert.Contains(t, names, "sub")
}

func TestBeamReadEndpointOpenRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := []byte("the quick brown fox")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "read.txt"), content, 0o644))

	readEP, _ := dialTestEndpoints(t, dir)

	rc, err := readEP.OpenRead("read.txt")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestBeamReadEndpointHash(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hash.txt"), []byte("hash me"), 0o644))

	readEP, _ := dialTestEndpoints(t, dir)

	hash, err := readEP.Hash("hash.txt")
	require.NoError(t, err)
	assert.Len(t, hash, 64) // BLAKE3 hex
}

func TestBeamWriteEndpointMkdirAll(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, writeEP := dialTestEndpoints(t, dir)

	require.NoError(t, writeEP.MkdirAll("a/b/c", 0o755))

	info, err := os.Stat(filepath.Join(dir, "a", "b", "c"))
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestBeamWriteEndpointCreateTempWriteRename(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, writeEP := dialTestEndpoints(t, dir)

	wf, err := writeEP.CreateTemp("output.txt", 0o644)
	require.NoError(t, err)

	_, err = wf.Write([]byte("beam protocol data"))
	require.NoError(t, err)

	require.NoError(t, wf.Close())
	require.NoError(t, writeEP.Rename(wf.Name(), "output.txt"))

	content, err := os.ReadFile(filepath.Join(dir, "output.txt"))
	require.NoError(t, err)
	assert.Equal(t, "beam protocol data", string(content))
}

func TestBeamWriteEndpointRemove(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "delete.txt"), []byte("bye"), 0o644))

	_, writeEP := dialTestEndpoints(t, dir)

	require.NoError(t, writeEP.Remove("delete.txt"))

	_, err := os.Stat(filepath.Join(dir, "delete.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestBeamWriteEndpointSymlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "target.txt"), []byte("target"), 0o644))

	_, writeEP := dialTestEndpoints(t, dir)

	require.NoError(t, writeEP.Symlink("target.txt", "link.txt"))

	linkTarget, err := os.Readlink(filepath.Join(dir, "link.txt"))
	require.NoError(t, err)
	assert.Equal(t, "target.txt", linkTarget)
}

func TestBeamWriteEndpointLink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "original.txt"), []byte("data"), 0o644))

	_, writeEP := dialTestEndpoints(t, dir)

	require.NoError(t, writeEP.Link("original.txt", "hardlink.txt"))

	content, err := os.ReadFile(filepath.Join(dir, "hardlink.txt"))
	require.NoError(t, err)
	assert.Equal(t, "data", string(content))
}

func TestBeamWriteEndpointSetMetadata(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "meta.txt"), []byte("x"), 0o644))

	_, writeEP := dialTestEndpoints(t, dir)

	entry := transport.FileEntry{
		Mode: 0o755,
	}
	opts := transport.MetadataOpts{Mode: true}

	require.NoError(t, writeEP.SetMetadata("meta.txt", entry, opts))

	info, err := os.Stat(filepath.Join(dir, "meta.txt"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}

func TestBeamEndpointCaps(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	readEP, _ := dialTestEndpoints(t, dir)

	caps := readEP.Caps()
	assert.True(t, caps.NativeHash)
	assert.True(t, caps.AtomicRename)
}

func TestDialBeamWrongToken(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	addr, _ := startTestDaemon(t, dir)

	_, _, _, err := beam.DialBeam(addr, "wrong-token", proto.ClientTLSConfig(true))
	assert.Error(t, err)
	// The error may be "authentication failed" or "connection closed during handshake"
	// depending on timing.
	assert.True(t,
		errors.Is(err, nil) == false,
		"expected error from wrong token, got nil")
}
