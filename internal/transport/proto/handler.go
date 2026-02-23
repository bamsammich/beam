package proto

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/bamsammich/beam/internal/transport"
)

// Handler dispatches incoming protocol frames to endpoint operations.
// One Handler is created per client connection.
type Handler struct {
	read  transport.ReadEndpoint
	write transport.WriteEndpoint

	mux *Mux

	// Open temp files tracked by handle ID.
	tempFiles sync.Map // handle string → transport.WriteFile
}

// NewHandler creates a new request handler backed by the given endpoints.
func NewHandler(read transport.ReadEndpoint, write transport.WriteEndpoint, mux *Mux) *Handler {
	return &Handler{
		read:  read,
		write: write,
		mux:   mux,
	}
}

// ServeStream handles all requests on a single stream until the stream channel
// is closed. Meant to be run as a goroutine — one per stream.
func (h *Handler) ServeStream(streamID uint32, ch <-chan Frame) {
	for f := range ch {
		if err := h.dispatch(streamID, f); err != nil {
			slog.Debug("handler error", "stream", streamID, "msg_type", f.MsgType, "error", err)
			h.sendError(streamID, err)
		}
	}
}

//nolint:gocyclo,revive // cyclomatic: protocol message dispatcher with one case per message type
func (h *Handler) dispatch(streamID uint32, f Frame) error {
	switch f.MsgType {
	// ReadEndpoint operations.
	case MsgWalkReq:
		return h.handleWalk(streamID)
	case MsgStatReq:
		return h.handleStat(streamID, f.Payload)
	case MsgReadDirReq:
		return h.handleReadDir(streamID, f.Payload)
	case MsgOpenReadReq:
		return h.handleOpenRead(streamID, f.Payload)
	case MsgHashReq:
		return h.handleHash(streamID, f.Payload)

	// WriteEndpoint operations.
	case MsgMkdirAllReq:
		return h.handleMkdirAll(streamID, f.Payload)
	case MsgCreateTempReq:
		return h.handleCreateTemp(streamID, f.Payload)
	case MsgWriteData:
		return h.handleWriteData(f.Payload)
	case MsgWriteDoneReq:
		return h.handleWriteDone(streamID, f.Payload)
	case MsgRenameReq:
		return h.handleRename(streamID, f.Payload)
	case MsgRemoveReq:
		return h.handleRemove(streamID, f.Payload)
	case MsgRemoveAllReq:
		return h.handleRemoveAll(streamID, f.Payload)
	case MsgSymlinkReq:
		return h.handleSymlink(streamID, f.Payload)
	case MsgLinkReq:
		return h.handleLink(streamID, f.Payload)
	case MsgSetMetadataReq:
		return h.handleSetMetadata(streamID, f.Payload)

	// Batch write operations.
	case MsgWriteFileBatchReq:
		return h.handleWriteFileBatch(streamID, f.Payload)

	// Delta transfer operations.
	case MsgComputeSignatureReq:
		return h.handleComputeSignature(streamID, f.Payload)
	case MsgMatchBlocksReq:
		return h.handleMatchBlocks(streamID, f.Payload)
	case MsgApplyDeltaReq:
		return h.handleApplyDelta(streamID, f.Payload)

	// Utility.
	case MsgCapsReq:
		return h.handleCaps(streamID)

	default:
		return fmt.Errorf("unknown message type: 0x%02x", f.MsgType)
	}
}

func (h *Handler) handleWalk(streamID uint32) error {
	err := h.read.Walk(func(entry transport.FileEntry) error {
		msg := WalkEntry{Entry: FromFileEntry(entry)}
		payload, err := msg.MarshalMsg(nil)
		if err != nil {
			return err
		}
		return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgWalkEntry, Payload: payload})
	})
	if err != nil {
		return err
	}

	end := WalkEnd{}
	payload, err := end.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgWalkEnd, Payload: payload})
}

func (h *Handler) handleStat(streamID uint32, data []byte) error {
	var req StatReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode StatReq: %w", err)
	}

	entry, err := h.read.Stat(req.RelPath)
	if err != nil {
		return err
	}

	resp := StatResp{Entry: FromFileEntry(entry)}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgStatResp, Payload: payload})
}

func (h *Handler) handleReadDir(streamID uint32, data []byte) error {
	var req ReadDirReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode ReadDirReq: %w", err)
	}

	entries, err := h.read.ReadDir(req.RelPath)
	if err != nil {
		return err
	}

	msgs := make([]FileEntryMsg, len(entries))
	for i, e := range entries {
		msgs[i] = FromFileEntry(e)
	}

	resp := ReadDirResp{Entries: msgs}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgReadDirResp, Payload: payload})
}

//nolint:revive // cognitive-complexity: streaming read with chunked send and EOF handling
func (h *Handler) handleOpenRead(streamID uint32, data []byte) error {
	var req OpenReadReq
	if _, unmarshalErr := req.UnmarshalMsg(data); unmarshalErr != nil {
		return fmt.Errorf("decode OpenReadReq: %w", unmarshalErr)
	}

	rc, err := h.read.OpenRead(req.RelPath)
	if err != nil {
		return err
	}
	defer rc.Close()

	buf := make([]byte, DataChunkSize)
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			msg := ReadDataMsg{Data: buf[:n]}
			payload, marshalErr := msg.MarshalMsg(nil)
			if marshalErr != nil {
				return marshalErr
			}
			if sendErr := h.mux.Send(
				Frame{StreamID: streamID, MsgType: MsgReadData, Payload: payload},
			); sendErr != nil {
				return sendErr
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	done := ReadDone{}
	payload, err := done.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgReadDone, Payload: payload})
}

func (h *Handler) handleHash(streamID uint32, data []byte) error {
	var req HashReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode HashReq: %w", err)
	}

	hash, err := h.read.Hash(req.RelPath)
	if err != nil {
		return err
	}

	resp := HashResp{Hash: hash}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgHashResp, Payload: payload})
}

func (h *Handler) handleMkdirAll(streamID uint32, data []byte) error {
	var req MkdirAllReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode MkdirAllReq: %w", err)
	}

	if err := h.write.MkdirAll(req.RelPath, os.FileMode(req.Perm)); err != nil {
		return err
	}

	return h.sendAck(streamID)
}

func (h *Handler) handleCreateTemp(streamID uint32, data []byte) error {
	var req CreateTempReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode CreateTempReq: %w", err)
	}

	wf, err := h.write.CreateTemp(req.RelPath, os.FileMode(req.Perm))
	if err != nil {
		return err
	}

	handle := fmt.Sprintf("h-%d-%s", streamID, wf.Name())
	h.tempFiles.Store(handle, wf)

	resp := CreateTempResp{Handle: handle, Name: wf.Name()}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgCreateTempResp, Payload: payload})
}

func (h *Handler) handleWriteData(data []byte) error {
	var msg WriteDataMsg
	if _, err := msg.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode WriteDataMsg: %w", err)
	}

	val, ok := h.tempFiles.Load(msg.Handle)
	if !ok {
		return fmt.Errorf("unknown temp file handle: %s", msg.Handle)
	}
	wf, _ := val.(transport.WriteFile) //nolint:revive // unchecked-type-assertion: type is guaranteed by handleCreateTemp

	_, err := wf.Write(msg.Data)
	return err
}

func (h *Handler) handleWriteDone(streamID uint32, data []byte) error {
	var req WriteDoneReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode WriteDoneReq: %w", err)
	}

	val, ok := h.tempFiles.LoadAndDelete(req.Handle)
	if !ok {
		return fmt.Errorf("unknown temp file handle: %s", req.Handle)
	}
	wf, _ := val.(transport.WriteFile) //nolint:revive // unchecked-type-assertion: type is guaranteed by handleCreateTemp

	if err := wf.Close(); err != nil {
		return err
	}

	resp := WriteDoneResp{}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgWriteDoneResp, Payload: payload})
}

func (h *Handler) handleRename(streamID uint32, data []byte) error {
	var req RenameReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode RenameReq: %w", err)
	}

	if err := h.write.Rename(req.OldRel, req.NewRel); err != nil {
		return err
	}
	return h.sendAck(streamID)
}

func (h *Handler) handleRemove(streamID uint32, data []byte) error {
	var req RemoveReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode RemoveReq: %w", err)
	}

	if err := h.write.Remove(req.RelPath); err != nil {
		return err
	}
	return h.sendAck(streamID)
}

func (h *Handler) handleRemoveAll(streamID uint32, data []byte) error {
	var req RemoveAllReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode RemoveAllReq: %w", err)
	}

	if err := h.write.RemoveAll(req.RelPath); err != nil {
		return err
	}
	return h.sendAck(streamID)
}

func (h *Handler) handleSymlink(streamID uint32, data []byte) error {
	var req SymlinkReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode SymlinkReq: %w", err)
	}

	if err := h.write.Symlink(req.Target, req.NewRel); err != nil {
		return err
	}
	return h.sendAck(streamID)
}

func (h *Handler) handleLink(streamID uint32, data []byte) error {
	var req LinkReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode LinkReq: %w", err)
	}

	if err := h.write.Link(req.OldRel, req.NewRel); err != nil {
		return err
	}
	return h.sendAck(streamID)
}

func (h *Handler) handleSetMetadata(streamID uint32, data []byte) error {
	var req SetMetadataReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode SetMetadataReq: %w", err)
	}

	entry := ToFileEntry(req.Entry)
	opts := ToMetadataOpts(req.Opts)

	if err := h.write.SetMetadata(req.RelPath, entry, opts); err != nil {
		return err
	}
	return h.sendAck(streamID)
}

func (h *Handler) handleCaps(streamID uint32) error {
	// Return capabilities from whichever endpoint is available.
	var caps transport.Capabilities
	if h.read != nil {
		caps = h.read.Caps()
	} else if h.write != nil {
		caps = h.write.Caps()
	}

	resp := FromCaps(caps)
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgCapsResp, Payload: payload})
}

func (h *Handler) handleComputeSignature(streamID uint32, data []byte) error {
	var req ComputeSignatureReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode ComputeSignatureReq: %w", err)
	}

	rc, err := h.read.OpenRead(req.RelPath)
	if err != nil {
		return err
	}
	defer rc.Close()

	sig, err := transport.ComputeSignature(rc, req.FileSize)
	if err != nil {
		return err
	}

	blockSize, sigMsgs := FromSignature(sig)
	resp := ComputeSignatureResp{
		BlockSize:  blockSize,
		Signatures: sigMsgs,
	}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgComputeSignatureResp, Payload: payload})
}

func (h *Handler) handleMatchBlocks(streamID uint32, data []byte) error {
	var req MatchBlocksReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode MatchBlocksReq: %w", err)
	}

	rc, err := h.read.OpenRead(req.RelPath)
	if err != nil {
		return err
	}
	defer rc.Close()

	sig := ToSignature(req.BlockSize, req.Signatures)
	ops, err := transport.MatchBlocks(rc, sig)
	if err != nil {
		return err
	}

	resp := MatchBlocksResp{Ops: FromDeltaOps(ops)}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgMatchBlocksResp, Payload: payload})
}

func (h *Handler) handleApplyDelta(streamID uint32, data []byte) error {
	var req ApplyDeltaReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode ApplyDeltaReq: %w", err)
	}

	// Open basis file for reading.
	basisRC, err := h.read.OpenRead(req.BasisRelPath)
	if err != nil {
		return err
	}
	defer basisRC.Close()

	basisSeeker, ok := basisRC.(io.ReadSeeker)
	if !ok {
		return errors.New("basis file does not support seeking")
	}

	// Open temp file for writing directly on the filesystem.
	// The temp file was created via CreateTemp and is relative to the write endpoint root.
	absPath := filepath.Join(h.write.Root(), req.TempRelPath)
	tmpFile, err := os.OpenFile(absPath, os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open temp file for delta: %w", err)
	}

	ops := ToDeltaOps(req.Ops)
	cw := &countingWriter{w: tmpFile}
	if applyErr := transport.ApplyDelta(basisSeeker, ops, cw); applyErr != nil {
		tmpFile.Close()
		return applyErr
	}
	if closeErr := tmpFile.Close(); closeErr != nil {
		return closeErr
	}

	resp := ApplyDeltaResp{BytesWritten: cw.n}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgApplyDeltaResp, Payload: payload})
}

// countingWriter wraps an io.Writer and counts bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

func (h *Handler) handleWriteFileBatch(streamID uint32, data []byte) error {
	var req WriteFileBatchReq
	if _, err := req.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("decode WriteFileBatchReq: %w", err)
	}

	opts := ToMetadataOpts(req.Opts)
	results := make([]WriteFileBatchResult, len(req.Entries))
	for i, entry := range req.Entries {
		if err := h.writeOneFile(entry, opts); err != nil {
			results[i] = WriteFileBatchResult{
				RelPath: entry.RelPath,
				OK:      false,
				Error:   err.Error(),
			}
		} else {
			results[i] = WriteFileBatchResult{
				RelPath: entry.RelPath,
				OK:      true,
			}
		}
	}

	resp := WriteFileBatchResp{Results: results}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgWriteFileBatchResp, Payload: payload})
}

// writeOneFile atomically writes a single small file: mkdir parent, create temp,
// write data, close, set metadata, rename. Temp file is cleaned up on failure.
func (h *Handler) writeOneFile(entry WriteFileBatchEntry, opts transport.MetadataOpts) error {
	// Ensure parent directory exists.
	relDir := filepath.Dir(entry.RelPath)
	if relDir != "." {
		if err := h.write.MkdirAll(relDir, 0755); err != nil {
			return fmt.Errorf("mkdir %s: %w", relDir, err)
		}
	}

	// Create temp file.
	absPath := filepath.Join(h.write.Root(), entry.RelPath)
	dir := filepath.Dir(absPath)
	base := filepath.Base(absPath)
	tmpName := fmt.Sprintf(".%s.%s.beam-tmp", base, uuid.New().String()[:8])
	tmpPath := filepath.Join(dir, tmpName)
	tmpRelPath := relPathFromRoot(h.write.Root(), tmpPath)

	perm := os.FileMode(entry.Perm)
	if perm == 0 {
		perm = 0644
	}

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	defer func() {
		// Best-effort cleanup on failure: remove temp if it still exists.
		os.Remove(tmpPath) //nolint:errcheck // best-effort cleanup of temp file on failure
	}()

	// Write data.
	if _, err := f.Write(entry.Data); err != nil {
		f.Close()
		return fmt.Errorf("write data: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	// Set metadata.
	fe := transport.FileEntry{
		Mode:    os.FileMode(entry.Mode),
		ModTime: time.Unix(0, entry.ModTime),
		AccTime: time.Unix(0, entry.AccTime),
		UID:     entry.UID,
		GID:     entry.GID,
	}
	if err := h.write.SetMetadata(tmpRelPath, fe, opts); err != nil {
		return fmt.Errorf("set metadata: %w", err)
	}

	// Atomic rename.
	if err := h.write.Rename(tmpRelPath, entry.RelPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	// Success — prevent deferred cleanup from removing the renamed file.
	// The deferred os.Remove will harmlessly fail since tmpPath no longer exists.
	return nil
}

func relPathFromRoot(root, absPath string) string {
	rel, err := filepath.Rel(root, absPath)
	if err != nil {
		return absPath
	}
	return rel
}

func (h *Handler) sendAck(streamID uint32) error {
	ack := AckResp{}
	payload, err := ack.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgAckResp, Payload: payload})
}

func (h *Handler) sendError(streamID uint32, origErr error) {
	resp := ErrorResp{Message: origErr.Error()}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		slog.Error("failed to marshal error response", "error", err)
		return
	}
	if err := h.mux.Send(
		Frame{StreamID: streamID, MsgType: MsgErrorResp, Payload: payload},
	); err != nil {
		slog.Error("failed to send error response", "error", err)
	}
}
