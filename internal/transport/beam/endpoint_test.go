package beam_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/beam"
	"github.com/bamsammich/beam/internal/transport/proto"
)

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

// startTestDaemon starts a daemon on a random port and returns connect info.
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

	go daemon.Serve(ctx) //nolint:errcheck // test daemon; error not needed

	return daemon.Addr().String(), authOpts
}

func dialTestEndpoints(
	t *testing.T,
	root string,
) (*beam.Reader, *beam.Writer) {
	t.Helper()

	addr, authOpts := startTestDaemon(t, root)

	mux, _, caps, err := beam.DialBeam(addr, authOpts, proto.ClientTLSConfig())
	require.NoError(t, err)
	t.Cleanup(func() { mux.Close() })

	readEP := beam.NewReader(mux, root, root, caps)
	writeEP := beam.NewWriter(mux, root, root, caps)
	return readEP, writeEP
}

func TestBeamReaderStat(t *testing.T) {
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

func TestBeamReaderWalk(t *testing.T) {
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

func TestBeamReaderReadDir(t *testing.T) {
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

func TestBeamReaderOpenRead(t *testing.T) {
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

func TestBeamReaderHash(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hash.txt"), []byte("hash me"), 0o644))

	readEP, _ := dialTestEndpoints(t, dir)

	hash, err := readEP.Hash("hash.txt")
	require.NoError(t, err)
	assert.Len(t, hash, 64) // BLAKE3 hex
}

func TestBeamWriterMkdirAll(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, writeEP := dialTestEndpoints(t, dir)

	require.NoError(t, writeEP.MkdirAll("a/b/c", 0o755))

	info, err := os.Stat(filepath.Join(dir, "a", "b", "c"))
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestBeamWriterCreateTempWriteRename(t *testing.T) {
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

func TestBeamWriterRemove(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "delete.txt"), []byte("bye"), 0o644))

	_, writeEP := dialTestEndpoints(t, dir)

	require.NoError(t, writeEP.Remove("delete.txt"))

	_, err := os.Stat(filepath.Join(dir, "delete.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestBeamWriterSymlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "target.txt"), []byte("target"), 0o644))

	_, writeEP := dialTestEndpoints(t, dir)

	require.NoError(t, writeEP.Symlink("target.txt", "link.txt"))

	linkTarget, err := os.Readlink(filepath.Join(dir, "link.txt"))
	require.NoError(t, err)
	assert.Equal(t, "target.txt", linkTarget)
}

func TestBeamWriterLink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "original.txt"), []byte("data"), 0o644))

	_, writeEP := dialTestEndpoints(t, dir)

	require.NoError(t, writeEP.Link("original.txt", "hardlink.txt"))

	content, err := os.ReadFile(filepath.Join(dir, "hardlink.txt"))
	require.NoError(t, err)
	assert.Equal(t, "data", string(content))
}

func TestBeamWriterSetMetadata(t *testing.T) {
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

func TestDialBeamAuthRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Start daemon that rejects all keys.
	daemon, err := proto.NewDaemon(proto.DaemonConfig{
		ListenAddr: "127.0.0.1:0",
		Root:       dir,
		KeyChecker: func(_ string, _ ssh.PublicKey) bool { return false },
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go daemon.Serve(ctx) //nolint:errcheck // test daemon

	authOpts := generateTestAuthOpts(t)
	_, _, _, dialErr := beam.DialBeam(daemon.Addr().String(), authOpts, proto.ClientTLSConfig())
	assert.Error(t, dialErr, "expected error from rejected auth, got nil")
}

// makeUniqueContent creates a byte slice where each block has unique content.
func makeUniqueContent(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func TestBeamReaderComputeSignature(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := makeUniqueContent(102400)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), content, 0o644))

	readEP, _ := dialTestEndpoints(t, dir)

	sig, err := readEP.ComputeSignature("data.bin", int64(len(content)))
	require.NoError(t, err)
	assert.Positive(t, sig.BlockSize)
	assert.NotEmpty(t, sig.Blocks)
}

func TestBeamReaderMatchBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := makeUniqueContent(102400)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "source.bin"), content, 0o644))

	// Compute sigs of slightly different version.
	modified := make([]byte, len(content))
	copy(modified, content)
	copy(modified[100:116], []byte("MODIFIED_BLOCK!!"))
	modSig, err := transport.ComputeSignature(bytes.NewReader(modified), int64(len(modified)))
	require.NoError(t, err)

	readEP, _ := dialTestEndpoints(t, dir)

	ops, err := readEP.MatchBlocks("source.bin", modSig)
	require.NoError(t, err)
	assert.NotEmpty(t, ops)

	hasBlock := false
	hasLiteral := false
	for _, op := range ops {
		if op.BlockIdx >= 0 {
			hasBlock = true
		} else {
			hasLiteral = true
		}
	}
	assert.True(t, hasBlock)
	assert.True(t, hasLiteral)
}

func TestBeamWriterComputeSignature(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := makeUniqueContent(102400)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), content, 0o644))

	_, writeEP := dialTestEndpoints(t, dir)

	sig, err := writeEP.ComputeSignature("data.bin", int64(len(content)))
	require.NoError(t, err)
	assert.Positive(t, sig.BlockSize)
	assert.NotEmpty(t, sig.Blocks)
}

func TestBeamWriterApplyDelta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Create basis file.
	basis := makeUniqueContent(102400)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "basis.bin"), basis, 0o644))

	// Compute sigs of basis, match against modified version.
	basisSig, err := transport.ComputeSignature(bytes.NewReader(basis), int64(len(basis)))
	require.NoError(t, err)

	modified := make([]byte, len(basis))
	copy(modified, basis)
	copy(modified[100:116], []byte("MODIFIED_BLOCK!!"))

	ops, err := transport.MatchBlocks(bytes.NewReader(modified), basisSig)
	require.NoError(t, err)

	_, writeEP := dialTestEndpoints(t, dir)

	// Create temp file.
	wf, err := writeEP.CreateTemp("basis.bin", 0o644)
	require.NoError(t, err)
	require.NoError(t, wf.Close())
	tmpName := wf.Name()

	// Apply delta.
	n, err := writeEP.ApplyDelta("basis.bin", tmpName, ops)
	require.NoError(t, err)
	assert.Equal(t, int64(len(modified)), n)

	// Rename and verify.
	require.NoError(t, writeEP.Rename(tmpName, "result.bin"))
	result, err := os.ReadFile(filepath.Join(dir, "result.bin"))
	require.NoError(t, err)
	assert.Equal(t, modified, result)
}
