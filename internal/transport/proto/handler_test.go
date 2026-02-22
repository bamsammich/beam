package proto_test

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/proto"
)

// testServer sets up a handler over net.Pipe and returns the client mux.
// The caller must call cleanup() when done.
func testServer(t *testing.T, root string) (clientMux *proto.Mux, cleanup func()) {
	t.Helper()

	clientConn, serverConn := net.Pipe()

	readEP := transport.NewLocalReadEndpoint(root)
	writeEP := transport.NewLocalWriteEndpoint(root)

	serverMux := proto.NewMux(serverConn)
	clientMux = proto.NewMux(clientConn)

	handler := proto.NewHandler(readEP, writeEP, serverMux)

	var wg sync.WaitGroup
	wg.Go(func() {
		serverMux.Run() //nolint:errcheck // mux.Run error propagated via mux closure
	})
	wg.Go(func() {
		clientMux.Run() //nolint:errcheck // mux.Run error propagated via mux closure
	})

	// Open a shared handler stream on the server side.
	serverCh := serverMux.OpenStream(1)
	go handler.ServeStream(1, serverCh)

	return clientMux, func() {
		clientMux.Close()
		serverMux.Close()
		wg.Wait()
		readEP.Close()
		writeEP.Close()
	}
}

// sendAndRecv is a helper that sends a request frame and waits for a response.
func sendAndRecv(
	t *testing.T,
	mux *proto.Mux,
	streamID uint32,
	ch <-chan proto.Frame,
	msgType byte,
	payload []byte,
) proto.Frame {
	t.Helper()
	require.NoError(t, mux.Send(proto.Frame{
		StreamID: streamID,
		MsgType:  msgType,
		Payload:  payload,
	}))

	select {
	case f := <-ch:
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for response")
		return proto.Frame{}
	}
}

func TestHandlerStat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello"), 0o644))

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	req := proto.StatReq{RelPath: "test.txt"}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgStatReq, payload)

	assert.Equal(t, proto.MsgStatResp, resp.MsgType)

	var statResp proto.StatResp
	_, err = statResp.UnmarshalMsg(resp.Payload)
	require.NoError(t, err)

	assert.Equal(t, "test.txt", statResp.Entry.RelPath)
	assert.Equal(t, int64(5), statResp.Entry.Size)
	assert.False(t, statResp.Entry.IsDir)
}

func TestHandlerMkdirAll(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	req := proto.MkdirAllReq{RelPath: "sub/dir", Perm: uint32(os.FileMode(0o755))}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgMkdirAllReq, payload)
	assert.Equal(t, proto.MsgAckResp, resp.MsgType)

	// Verify directory was created.
	info, err := os.Stat(filepath.Join(dir, "sub", "dir"))
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestHandlerCreateTempWriteRename(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	// CreateTemp
	createReq := proto.CreateTempReq{RelPath: "output.txt", Perm: uint32(os.FileMode(0o644))}
	payload, err := createReq.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgCreateTempReq, payload)
	assert.Equal(t, proto.MsgCreateTempResp, resp.MsgType)

	var createResp proto.CreateTempResp
	_, err = createResp.UnmarshalMsg(resp.Payload)
	require.NoError(t, err)
	assert.NotEmpty(t, createResp.Handle)
	assert.NotEmpty(t, createResp.Name)

	// WriteData
	writeMsg := proto.WriteDataMsg{Handle: createResp.Handle, Data: []byte("file content here")}
	writePayload, err := writeMsg.MarshalMsg(nil)
	require.NoError(t, err)

	require.NoError(t, mux.Send(proto.Frame{
		StreamID: 1,
		MsgType:  proto.MsgWriteData,
		Payload:  writePayload,
	}))

	// WriteDone
	doneReq := proto.WriteDoneReq{Handle: createResp.Handle}
	donePayload, err := doneReq.MarshalMsg(nil)
	require.NoError(t, err)

	resp = sendAndRecv(t, mux, 1, ch, proto.MsgWriteDoneReq, donePayload)
	assert.Equal(t, proto.MsgWriteDoneResp, resp.MsgType)

	// Rename temp → final
	renameReq := proto.RenameReq{OldRel: createResp.Name, NewRel: "output.txt"}
	renamePayload, err := renameReq.MarshalMsg(nil)
	require.NoError(t, err)

	resp = sendAndRecv(t, mux, 1, ch, proto.MsgRenameReq, renamePayload)
	assert.Equal(t, proto.MsgAckResp, resp.MsgType)

	// Verify final file content.
	content, err := os.ReadFile(filepath.Join(dir, "output.txt"))
	require.NoError(t, err)
	assert.Equal(t, "file content here", string(content))
}

//nolint:revive // cognitive-complexity: test collects streamed walk entries with timeout
func TestHandlerWalk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sub", "b.txt"), []byte("bb"), 0o644))

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	req := proto.WalkReq{}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	require.NoError(t, mux.Send(proto.Frame{
		StreamID: 1,
		MsgType:  proto.MsgWalkReq,
		Payload:  payload,
	}))

	// Collect entries until WalkEnd.
	var entries []proto.FileEntryMsg
	for {
		select {
		case f := <-ch:
			if f.MsgType == proto.MsgWalkEnd {
				goto done
			}
			require.Equal(t, proto.MsgWalkEntry, f.MsgType)
			var entry proto.WalkEntry
			_, err := entry.UnmarshalMsg(f.Payload)
			require.NoError(t, err)
			entries = append(entries, entry.Entry)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for walk entries")
		}
	}
done:

	// Should have at least the directory + 2 files.
	paths := make(map[string]bool)
	for _, e := range entries {
		paths[e.RelPath] = true
	}

	assert.True(t, paths["a.txt"], "expected a.txt in walk results")
	assert.True(t, paths["sub/b.txt"] || paths[filepath.Join("sub", "b.txt")],
		"expected sub/b.txt in walk results, got %v", paths)
}

func TestHandlerOpenRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := []byte("the quick brown fox jumps over the lazy dog")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "read.txt"), content, 0o644))

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	req := proto.OpenReadReq{RelPath: "read.txt"}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	require.NoError(t, mux.Send(proto.Frame{
		StreamID: 1,
		MsgType:  proto.MsgOpenReadReq,
		Payload:  payload,
	}))

	// Collect data chunks until ReadDone.
	var received []byte
	for {
		select {
		case f := <-ch:
			if f.MsgType == proto.MsgReadDone {
				goto done
			}
			require.Equal(t, proto.MsgReadData, f.MsgType)
			var msg proto.ReadDataMsg
			_, err := msg.UnmarshalMsg(f.Payload)
			require.NoError(t, err)
			received = append(received, msg.Data...)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for read data")
		}
	}
done:

	assert.Equal(t, content, received)
}

func TestHandlerHash(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hash.txt"), []byte("hash me"), 0o644))

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	req := proto.HashReq{RelPath: "hash.txt"}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgHashReq, payload)
	assert.Equal(t, proto.MsgHashResp, resp.MsgType)

	var hashResp proto.HashResp
	_, err = hashResp.UnmarshalMsg(resp.Payload)
	require.NoError(t, err)
	assert.NotEmpty(t, hashResp.Hash)
	assert.Len(t, hashResp.Hash, 64) // BLAKE3 hex = 64 chars
}

func TestHandlerRemove(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "delete.txt"), []byte("bye"), 0o644))

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	req := proto.RemoveReq{RelPath: "delete.txt"}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgRemoveReq, payload)
	assert.Equal(t, proto.MsgAckResp, resp.MsgType)

	_, err = os.Stat(filepath.Join(dir, "delete.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestHandlerCaps(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	req := proto.CapsReq{}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgCapsReq, payload)
	assert.Equal(t, proto.MsgCapsResp, resp.MsgType)

	var capsResp proto.CapsResp
	_, err = capsResp.UnmarshalMsg(resp.Payload)
	require.NoError(t, err)

	// Local endpoints have all caps true.
	assert.True(t, capsResp.NativeHash)
	assert.True(t, capsResp.AtomicRename)
}

func TestHandlerErrorResponse(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	// Stat a nonexistent file — should get ErrorResp.
	req := proto.StatReq{RelPath: "nonexistent.txt"}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgStatReq, payload)
	assert.Equal(t, proto.MsgErrorResp, resp.MsgType)

	var errResp proto.ErrorResp
	_, err = errResp.UnmarshalMsg(resp.Payload)
	require.NoError(t, err)
	assert.Contains(t, errResp.Message, "nonexistent.txt")
}

func TestHandlerComputeSignature(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Create a 100KB file — big enough for meaningful block signatures.
	content := make([]byte, 102400)
	for i := range content {
		content[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "basis.bin"), content, 0o644))

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	req := proto.ComputeSignatureReq{RelPath: "basis.bin", FileSize: int64(len(content))}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgComputeSignatureReq, payload)
	assert.Equal(t, proto.MsgComputeSignatureResp, resp.MsgType)

	var sigResp proto.ComputeSignatureResp
	_, err = sigResp.UnmarshalMsg(resp.Payload)
	require.NoError(t, err)

	assert.Positive(t, sigResp.BlockSize)
	assert.NotEmpty(t, sigResp.Signatures)
	// Each sig should have a 32-byte strong hash.
	for _, sig := range sigResp.Signatures {
		assert.Len(t, sig.StrongHash, 32)
	}
}

func TestHandlerMatchBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Create a file with unique content per block so blocks are distinguishable.
	content := make([]byte, 102400)
	for i := range content {
		content[i] = byte(i % 251) // prime modulus makes blocks unique
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "source.bin"), content, 0o644))

	// Compute signatures of a slightly different version locally.
	modified := make([]byte, len(content))
	copy(modified, content)
	copy(modified[100:116], []byte("MODIFIED_BLOCK!!"))
	modSig, err := transport.ComputeSignature(bytes.NewReader(modified), int64(len(modified)))
	require.NoError(t, err)

	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	blockSize, sigMsgs := proto.FromSignature(modSig)
	req := proto.MatchBlocksReq{
		RelPath:    "source.bin",
		BlockSize:  blockSize,
		Signatures: sigMsgs,
	}
	payload, err := req.MarshalMsg(nil)
	require.NoError(t, err)

	resp := sendAndRecv(t, mux, 1, ch, proto.MsgMatchBlocksReq, payload)
	assert.Equal(t, proto.MsgMatchBlocksResp, resp.MsgType)

	var matchResp proto.MatchBlocksResp
	_, err = matchResp.UnmarshalMsg(resp.Payload)
	require.NoError(t, err)

	assert.NotEmpty(t, matchResp.Ops)

	// Should have a mix: at least one block reference and at least one literal
	// (since the source differs from the modified version we computed sigs for).
	hasBlock := false
	hasLiteral := false
	for _, op := range matchResp.Ops {
		if op.BlockIdx >= 0 {
			hasBlock = true
		} else {
			hasLiteral = true
		}
	}
	assert.True(t, hasBlock, "expected at least one block reference")
	assert.True(t, hasLiteral, "expected at least one literal op")
}

func TestHandlerApplyDelta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Create basis file with unique content per block.
	basis := make([]byte, 102400)
	for i := range basis {
		basis[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "basis.bin"), basis, 0o644))

	// Compute sigs of basis, match against a modified version, get ops.
	basisSig, err := transport.ComputeSignature(bytes.NewReader(basis), int64(len(basis)))
	require.NoError(t, err)

	modified := make([]byte, len(basis))
	copy(modified, basis)
	copy(modified[100:116], []byte("MODIFIED_BLOCK!!"))

	ops, err := transport.MatchBlocks(bytes.NewReader(modified), basisSig)
	require.NoError(t, err)

	// Create temp file via handler (use CreateTemp first).
	mux, cleanup := testServer(t, dir)
	defer cleanup()

	ch := mux.OpenStream(1)
	defer mux.CloseStream(1)

	createReq := proto.CreateTempReq{RelPath: "basis.bin", Perm: uint32(os.FileMode(0o644))}
	createPayload, err := createReq.MarshalMsg(nil)
	require.NoError(t, err)

	createResp := sendAndRecv(t, mux, 1, ch, proto.MsgCreateTempReq, createPayload)
	require.Equal(t, proto.MsgCreateTempResp, createResp.MsgType)

	var tempResp proto.CreateTempResp
	_, err = tempResp.UnmarshalMsg(createResp.Payload)
	require.NoError(t, err)

	// Close the temp file handle so it can be opened by ApplyDelta.
	doneReq := proto.WriteDoneReq{Handle: tempResp.Handle}
	donePayload, err := doneReq.MarshalMsg(nil)
	require.NoError(t, err)
	doneFrame := sendAndRecv(t, mux, 1, ch, proto.MsgWriteDoneReq, donePayload)
	require.Equal(t, proto.MsgWriteDoneResp, doneFrame.MsgType)

	// Now apply delta.
	applyReq := proto.ApplyDeltaReq{
		BasisRelPath: "basis.bin",
		TempRelPath:  tempResp.Name,
		Ops:          proto.FromDeltaOps(ops),
	}
	applyPayload, err := applyReq.MarshalMsg(nil)
	require.NoError(t, err)

	applyResp := sendAndRecv(t, mux, 1, ch, proto.MsgApplyDeltaReq, applyPayload)
	assert.Equal(t, proto.MsgApplyDeltaResp, applyResp.MsgType)

	var deltaResp proto.ApplyDeltaResp
	_, err = deltaResp.UnmarshalMsg(applyResp.Payload)
	require.NoError(t, err)
	assert.Equal(t, int64(len(modified)), deltaResp.BytesWritten)

	// Verify the temp file has correct content.
	result, err := os.ReadFile(filepath.Join(dir, tempResp.Name))
	require.NoError(t, err)
	assert.Equal(t, modified, result)
}
