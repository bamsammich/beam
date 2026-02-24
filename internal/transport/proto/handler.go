package proto

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/bamsammich/beam/internal/transport"
)

// batchFileSeq is an atomic counter for generating unique temp file names
// within batch writes, replacing per-file UUID generation.
var batchFileSeq atomic.Uint64

// batchWriteParallel is the number of concurrent goroutines used to process
// files within a single batch write request on the server.
const batchWriteParallel = 16

// Handler dispatches incoming protocol frames to endpoint operations.
// One Handler is created per client connection.
type Handler struct {
	read  transport.Reader
	write transport.ReadWriter

	mux *Mux

	// Open temp files tracked by handle ID.
	tempFiles sync.Map // handle string → transport.WriteFile
}

// NewHandler creates a new request handler backed by the given endpoints.
func NewHandler(read transport.Reader, write transport.ReadWriter, mux *Mux) *Handler {
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
	// Reader operations.
	case MsgWalkReq:
		return h.handleWalk(streamID, f.Payload)
	case MsgStatReq:
		return h.handleStat(streamID, f.Payload)
	case MsgReadDirReq:
		return h.handleReadDir(streamID, f.Payload)
	case MsgOpenReadReq:
		return h.handleOpenRead(streamID, f.Payload)
	case MsgHashReq:
		return h.handleHash(streamID, f.Payload)

	// ReadWriter operations.
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

func (h *Handler) handleWalk(streamID uint32, data []byte) error {
	var req WalkReq
	if len(data) > 0 {
		if _, err := req.UnmarshalMsg(data); err != nil {
			return fmt.Errorf("decode WalkReq: %w", err)
		}
	}

	// If RelPath is set, walk just that subtree using direct filesystem access.
	// Entry RelPaths in the response are relative to the walk root.
	if req.RelPath != "" {
		return h.handleWalkSubtree(streamID, req.RelPath)
	}

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

//nolint:revive // cognitive-complexity: subtree walk with stat and entry emission
func (h *Handler) handleWalkSubtree(streamID uint32, relPath string) error {
	root := filepath.Join(h.read.Root(), relPath)

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil // skip inaccessible entries
		}
		entryRel, relErr := filepath.Rel(root, path)
		if relErr != nil || entryRel == "." {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return nil
		}
		entry := transport.FileEntry{
			RelPath: entryRel,
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		}
		if info.Mode()&os.ModeSymlink != 0 {
			entry.IsSymlink = true
			if target, err := os.Readlink(path); err == nil {
				entry.LinkTarget = target
			}
		}

		msg := WalkEntry{Entry: FromFileEntry(entry)}
		payload, marshalErr := msg.MarshalMsg(nil)
		if marshalErr != nil {
			return marshalErr
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

	type readDirer interface {
		ReadDir(relPath string) ([]transport.FileEntry, error)
	}
	rd, ok := h.read.(readDirer)
	if !ok {
		return errors.New("endpoint does not support ReadDir")
	}
	entries, err := rd.ReadDir(req.RelPath)
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
	root := h.write.Root()

	// Pre-create all unique parent directories before parallel file writes.
	dirs := make(map[string]struct{})
	for _, e := range req.Entries {
		dir := filepath.Dir(filepath.Join(root, e.RelPath))
		dirs[dir] = struct{}{}
	}
	for dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			slog.Debug("batch mkdir", "dir", dir, "error", err)
		}
	}

	// Process entries in parallel with bounded concurrency.
	results := make([]WriteFileBatchResult, len(req.Entries))
	var wg sync.WaitGroup
	sem := make(chan struct{}, batchWriteParallel)

	for i, entry := range req.Entries {
		sem <- struct{}{} // backpressure: block until a slot opens
		idx, e := i, entry
		wg.Go(func() {
			defer func() { <-sem }()
			if err := batchWriteOneFile(root, e, opts); err != nil {
				results[idx] = WriteFileBatchResult{
					RelPath: e.RelPath,
					OK:      false,
					Error:   err.Error(),
				}
			} else {
				results[idx] = WriteFileBatchResult{
					RelPath: e.RelPath,
					OK:      true,
				}
			}
		})
	}
	wg.Wait()

	resp := WriteFileBatchResp{Results: results}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return h.mux.Send(Frame{StreamID: streamID, MsgType: MsgWriteFileBatchResp, Payload: payload})
}

// batchWriteOneFile atomically writes a single small file using direct
// syscalls for efficiency: create temp, write data, set metadata via fd,
// close, rename. Parent directories must already exist (pre-created by
// handleWriteFileBatch).
//
//nolint:gocyclo,revive // cyclomatic: create + write + fd-metadata + close + rename with error handling
func batchWriteOneFile(root string, entry WriteFileBatchEntry, opts transport.MetadataOpts) error {
	absPath := filepath.Join(root, entry.RelPath)
	dir := filepath.Dir(absPath)
	base := filepath.Base(absPath)

	// Atomic counter for temp names (cheaper than UUID generation).
	seq := batchFileSeq.Add(1)
	tmpPath := filepath.Join(dir, fmt.Sprintf(".%s.%x.beam-tmp", base, seq))

	perm := os.FileMode(entry.Perm)
	if perm == 0 {
		perm = 0644
	}

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}

	success := false
	defer func() {
		if !success {
			os.Remove(tmpPath) //nolint:errcheck // best-effort cleanup on failure
		}
	}()

	// Write data.
	if _, err := f.Write(entry.Data); err != nil {
		f.Close()
		return fmt.Errorf("write data: %w", err)
	}

	// Set metadata via fd-based syscalls (avoids extra path lookups).
	rawFd := int(f.Fd()) //nolint:gosec // G115: fd conversion safe for file descriptors
	if opts.Mode {
		if err := unix.Fchmod(rawFd, entry.Mode&0o7777); err != nil {
			f.Close()
			return fmt.Errorf("fchmod: %w", err)
		}
	}
	if opts.Times {
		atime := unix.NsecToTimespec(entry.AccTime)
		mtime := unix.NsecToTimespec(entry.ModTime)
		if err := unix.UtimesNanoAt(
			unix.AT_FDCWD, tmpPath, []unix.Timespec{atime, mtime}, 0,
		); err != nil {
			f.Close()
			return fmt.Errorf("utimensat: %w", err)
		}
	}
	if opts.Owner {
		//nolint:errcheck,gosec // best-effort; may fail without CAP_CHOWN; uid/gid widening safe
		_ = unix.Fchown(rawFd, int(entry.UID), int(entry.GID))
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, absPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	success = true
	return nil
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
