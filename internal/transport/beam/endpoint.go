// Package beam implements ReadEndpoint and WriteEndpoint over the beam binary protocol.
package beam

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/proto"
)

// computePathPrefix computes the path prefix that must be prepended to all
// relPaths before sending them to the server. The server resolves paths
// relative to its daemon root, but the client sends paths relative to
// root (loc.Path). The prefix bridges the gap.
//
// Example: daemonRoot="/", root="/home/thudd/dst" → prefix="home/thudd/dst"
// Then relPath "file.txt" → server sees "home/thudd/dst/file.txt" → resolves to
// "/home/thudd/dst/file.txt" on the server.
//
// When root is outside the daemon root (Rel produces ".."), we return ""
// to map to the daemon root itself. This handles the common case of
// beam://token@host:port/ where loc.Path="/" and the daemon root is a
// specific directory — the user wants the daemon's served content.
func computePathPrefix(root, daemonRoot string) string {
	rel, err := filepath.Rel(daemonRoot, root)
	if err != nil || rel == "." {
		return ""
	}
	if strings.HasPrefix(rel, "..") {
		return ""
	}
	return rel
}

// serverPath translates a client-relative path to a server-relative path
// by prepending the path prefix.
func serverPath(prefix, relPath string) string {
	if prefix == "" {
		return relPath
	}
	return filepath.Join(prefix, relPath)
}

// clientPath translates a server-relative path back to a client-relative path
// by stripping the path prefix.
func clientPath(prefix, serverRelPath string) string {
	if prefix == "" {
		return serverRelPath
	}
	trimmed := strings.TrimPrefix(serverRelPath, prefix)
	trimmed = strings.TrimPrefix(trimmed, "/")
	if trimmed == "" {
		return "."
	}
	return trimmed
}

// DefaultPort is the default beam daemon port.
const DefaultPort = 9876

// DialBeam connects to a beam daemon over TLS, performs protocol handshake.
// Returns a running mux, the server root, and capabilities.
//
//nolint:revive // function-result-limit: established API returning (mux, root, caps, err)
func DialBeam(
	addr, token string,
	tlsConfig *tls.Config,
) (*proto.Mux, string, transport.Capabilities, error) {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, tlsConfig)
	if err != nil {
		return nil, "", transport.Capabilities{}, fmt.Errorf("dial %s: %w", addr, err)
	}

	return DialBeamConn(conn, token)
}

// DialBeamConn performs the beam protocol handshake over an already-established
// connection. This is the core handshake logic shared by DialBeam (direct TLS)
// and DialBeamTunnel (SSH-tunneled TLS). Returns a running mux, the server
// root, and capabilities.
//
//nolint:revive // cyclomatic: handshake + response parsing — irreducible
func DialBeamConn(
	conn net.Conn, token string,
) (*proto.Mux, string, transport.Capabilities, error) {
	mux := proto.NewMux(conn)

	// Start mux in background.
	go mux.Run() //nolint:errcheck // mux.Run error propagated via mux closure

	// Open control stream for handshake.
	controlCh := mux.OpenStream(proto.ControlStream)

	req := proto.HandshakeReq{
		Version:   proto.ProtocolVersion,
		AuthToken: token,
	}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		mux.Close()
		return nil, "", transport.Capabilities{}, fmt.Errorf("marshal handshake: %w", err)
	}

	if err := mux.Send(proto.Frame{
		StreamID: proto.ControlStream,
		MsgType:  proto.MsgHandshakeReq,
		Payload:  payload,
	}); err != nil {
		mux.Close()
		return nil, "", transport.Capabilities{}, fmt.Errorf("send handshake: %w", err)
	}

	// Wait for response.
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case f, ok := <-controlCh:
		if !ok {
			mux.Close()
			return nil, "", transport.Capabilities{}, errors.New(
				"connection closed during handshake",
			)
		}
		if f.MsgType == proto.MsgErrorResp {
			var errResp proto.ErrorResp
			errResp.UnmarshalMsg(f.Payload) //nolint:errcheck // best effort
			mux.Close()
			return nil, "", transport.Capabilities{}, fmt.Errorf(
				"handshake rejected: %s",
				errResp.Message,
			)
		}
		if f.MsgType == 0 && len(f.Payload) == 0 {
			// Zero-value frame from closed channel.
			mux.Close()
			return nil, "", transport.Capabilities{}, errors.New(
				"connection closed during handshake (auth rejected)",
			)
		}
		if f.MsgType != proto.MsgHandshakeResp {
			mux.Close()
			return nil, "", transport.Capabilities{}, fmt.Errorf(
				"unexpected message type 0x%02x during handshake",
				f.MsgType,
			)
		}

		var resp proto.HandshakeResp
		if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
			mux.Close()
			return nil, "", transport.Capabilities{}, fmt.Errorf(
				"decode handshake response: %w",
				err,
			)
		}

		mux.CloseStream(proto.ControlStream)

		// Query capabilities.
		caps, err := queryCaps(mux)
		if err != nil {
			mux.Close()
			return nil, "", transport.Capabilities{}, fmt.Errorf("query capabilities: %w", err)
		}

		return mux, resp.Root, caps, nil

	case <-timer.C:
		mux.Close()
		return nil, "", transport.Capabilities{}, errors.New("handshake timeout")
	}
}

func queryCaps(mux *proto.Mux) (transport.Capabilities, error) {
	streamID := uint32(1)
	ch := mux.OpenStream(streamID)
	defer mux.CloseStream(streamID)

	req := proto.CapsReq{}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return transport.Capabilities{}, err
	}

	if err := mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgCapsReq, Payload: payload},
	); err != nil {
		return transport.Capabilities{}, err
	}

	f, ok := <-ch
	if !ok {
		return transport.Capabilities{}, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return transport.Capabilities{}, decodeError(f.Payload)
	}

	var resp proto.CapsResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return transport.Capabilities{}, err
	}
	return proto.ToCaps(resp), nil
}

func decodeError(payload []byte) error {
	var errResp proto.ErrorResp
	if _, err := errResp.UnmarshalMsg(payload); err != nil {
		return errors.New("protocol error (could not decode)")
	}
	return errors.New(errResp.Message)
}

// ReadEndpoint implements transport.ReadEndpoint over the beam protocol.
type ReadEndpoint struct {
	mux        *proto.Mux
	root       string // client-side root (loc.Path)
	pathPrefix string // prefix to prepend to relPaths for server resolution
	caps       transport.Capabilities
	nextStream atomic.Uint32
}

// Compile-time interface check.
var _ transport.ReadEndpoint = (*ReadEndpoint)(nil)

// NewReadEndpoint creates a read endpoint from an established mux connection.
// root is the client's target path (loc.Path), daemonRoot is the server's root.
// All relPaths sent to the server are prefixed with the path from daemonRoot to root.
func NewReadEndpoint(
	mux *proto.Mux,
	root, daemonRoot string,
	caps transport.Capabilities,
) *ReadEndpoint {
	prefix := computePathPrefix(root, daemonRoot)
	ep := &ReadEndpoint{mux: mux, root: root, pathPrefix: prefix, caps: caps}
	ep.nextStream.Store(2) // 1 was used for caps query
	return ep
}

func (e *ReadEndpoint) allocStream() uint32 {
	return e.nextStream.Add(1)
}

func (e *ReadEndpoint) Walk(fn func(entry transport.FileEntry) error) error {
	return e.walkRel("", fn)
}

// walkRel walks a subtree starting at walkRoot (relative to endpoint root).
// An empty walkRoot walks the entire endpoint root.
func (e *ReadEndpoint) walkRel(walkRoot string, fn func(entry transport.FileEntry) error) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.WalkReq{RelPath: serverPath(e.pathPrefix, walkRoot)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgWalkReq, Payload: payload},
	); err != nil {
		return err
	}

	// The server returns entries with RelPath relative to the walk root.
	for f := range ch {
		switch f.MsgType {
		case proto.MsgWalkEntry:
			var entry proto.WalkEntry
			if _, err := entry.UnmarshalMsg(f.Payload); err != nil {
				return err
			}
			if err := fn(proto.ToFileEntry(entry.Entry)); err != nil {
				return err
			}
		case proto.MsgWalkEnd:
			return nil
		case proto.MsgErrorResp:
			return decodeError(f.Payload)
		default:
			return fmt.Errorf("unexpected message type 0x%02x during walk", f.MsgType)
		}
	}
	return errors.New("stream closed during walk")
}

func (e *ReadEndpoint) Stat(relPath string) (transport.FileEntry, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.StatReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return transport.FileEntry{}, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgStatReq, Payload: payload},
	); err != nil {
		return transport.FileEntry{}, err
	}

	f, ok := <-ch
	if !ok {
		return transport.FileEntry{}, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return transport.FileEntry{}, decodeError(f.Payload)
	}

	var resp proto.StatResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return transport.FileEntry{}, err
	}
	entry := proto.ToFileEntry(resp.Entry)
	entry.RelPath = relPath // restore client-relative path
	return entry, nil
}

func (e *ReadEndpoint) ReadDir(relPath string) ([]transport.FileEntry, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.ReadDirReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgReadDirReq, Payload: payload},
	); err != nil {
		return nil, err
	}

	f, ok := <-ch
	if !ok {
		return nil, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return nil, decodeError(f.Payload)
	}

	var resp proto.ReadDirResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return nil, err
	}

	entries := make([]transport.FileEntry, len(resp.Entries))
	for i, m := range resp.Entries {
		entries[i] = proto.ToFileEntry(m)
	}
	return entries, nil
}

func (e *ReadEndpoint) OpenRead(relPath string) (io.ReadCloser, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)

	req := proto.OpenReadReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgOpenReadReq, Payload: payload},
	); err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}

	return &beamReader{mux: e.mux, streamID: streamID, ch: ch}, nil
}

func (e *ReadEndpoint) Hash(relPath string) (string, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.HashReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return "", err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgHashReq, Payload: payload},
	); err != nil {
		return "", err
	}

	f, ok := <-ch
	if !ok {
		return "", errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return "", decodeError(f.Payload)
	}

	var resp proto.HashResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return "", err
	}
	return resp.Hash, nil
}

// ComputeSignature asks the remote daemon to compute block signatures of a file.
func (e *ReadEndpoint) ComputeSignature(
	relPath string,
	fileSize int64,
) (transport.Signature, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.ComputeSignatureReq{RelPath: serverPath(e.pathPrefix, relPath), FileSize: fileSize}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return transport.Signature{}, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgComputeSignatureReq, Payload: payload},
	); err != nil {
		return transport.Signature{}, err
	}

	f, ok := <-ch
	if !ok {
		return transport.Signature{}, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return transport.Signature{}, decodeError(f.Payload)
	}
	var resp proto.ComputeSignatureResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return transport.Signature{}, err
	}
	return proto.ToSignature(resp.BlockSize, resp.Signatures), nil
}

// MatchBlocks asks the remote daemon to match its file against provided signatures.
func (e *ReadEndpoint) MatchBlocks(
	relPath string,
	sig transport.Signature,
) ([]transport.DeltaOp, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	blockSize, sigMsgs := proto.FromSignature(sig)
	req := proto.MatchBlocksReq{
		RelPath:    serverPath(e.pathPrefix, relPath),
		BlockSize:  blockSize,
		Signatures: sigMsgs,
	}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgMatchBlocksReq, Payload: payload},
	); err != nil {
		return nil, err
	}

	f, ok := <-ch
	if !ok {
		return nil, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return nil, decodeError(f.Payload)
	}
	var resp proto.MatchBlocksResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return nil, err
	}
	return proto.ToDeltaOps(resp.Ops), nil
}

func (e *ReadEndpoint) Root() string                 { return e.root }
func (e *ReadEndpoint) Caps() transport.Capabilities { return e.caps }
func (e *ReadEndpoint) Close() error                 { return e.mux.Close() }

// beamReader implements io.ReadCloser by reading streamed data from the protocol.
type beamReader struct {
	mux      *proto.Mux
	ch       <-chan proto.Frame
	buf      []byte // unconsumed data from last ReadDataMsg
	streamID uint32
	done     bool
}

//nolint:revive // cyclomatic: select + switch for protocol dispatch with mux cancellation — irreducible
func (r *beamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}

		select {
		case f, ok := <-r.ch:
			if !ok {
				r.done = true
				return 0, io.EOF
			}

			switch f.MsgType {
			case proto.MsgReadData:
				var msg proto.ReadDataMsg
				if _, err := msg.UnmarshalMsg(f.Payload); err != nil {
					return 0, err
				}
				r.buf = msg.Data
			case proto.MsgReadDone:
				r.done = true
				return 0, io.EOF
			case proto.MsgErrorResp:
				return 0, decodeError(f.Payload)
			default:
				return 0, fmt.Errorf("unexpected message type 0x%02x during read", f.MsgType)
			}
		case <-r.mux.Done():
			r.done = true
			return 0, io.EOF
		}
	}

	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *beamReader) Close() error {
	r.mux.CloseStream(r.streamID)
	return nil
}

// WriteEndpoint implements transport.WriteEndpoint over the beam protocol.
type WriteEndpoint struct {
	mux        *proto.Mux
	root       string // client-side root (loc.Path)
	pathPrefix string // prefix to prepend to relPaths for server resolution
	caps       transport.Capabilities
	nextStream atomic.Uint32
}

// Compile-time interface check.
var _ transport.WriteEndpoint = (*WriteEndpoint)(nil)

// NewWriteEndpoint creates a write endpoint from an established mux connection.
// root is the client's target path (loc.Path), daemonRoot is the server's root.
func NewWriteEndpoint(
	mux *proto.Mux,
	root, daemonRoot string,
	caps transport.Capabilities,
) *WriteEndpoint {
	prefix := computePathPrefix(root, daemonRoot)
	ep := &WriteEndpoint{mux: mux, root: root, pathPrefix: prefix, caps: caps}
	ep.nextStream.Store(2)
	return ep
}

func (e *WriteEndpoint) allocStream() uint32 {
	return e.nextStream.Add(1)
}

func (e *WriteEndpoint) MkdirAll(relPath string, perm os.FileMode) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.MkdirAllReq{RelPath: serverPath(e.pathPrefix, relPath), Perm: uint32(perm)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgMkdirAllReq, Payload: payload},
	); err != nil {
		return err
	}

	return e.expectAck(ch)
}

//nolint:ireturn // implements WriteEndpoint interface
func (e *WriteEndpoint) CreateTemp(
	relPath string,
	perm os.FileMode,
) (transport.WriteFile, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)

	req := proto.CreateTempReq{RelPath: serverPath(e.pathPrefix, relPath), Perm: uint32(perm)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgCreateTempReq, Payload: payload},
	); err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}

	f, ok := <-ch
	if !ok {
		e.mux.CloseStream(streamID)
		return nil, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		e.mux.CloseStream(streamID)
		return nil, decodeError(f.Payload)
	}

	var resp proto.CreateTempResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}

	// Keep the stream open — WriteData and WriteDone reuse it so that the
	// server handler processes chunks sequentially on one goroutine.
	// Translate server-relative name back to client-relative.
	return &beamWriteFile{
		mux:      e.mux,
		streamID: streamID,
		ch:       ch,
		handle:   resp.Handle,
		name:     clientPath(e.pathPrefix, resp.Name),
	}, nil
}

func (e *WriteEndpoint) Rename(oldRel, newRel string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.RenameReq{
		OldRel: serverPath(e.pathPrefix, oldRel),
		NewRel: serverPath(e.pathPrefix, newRel),
	}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgRenameReq, Payload: payload},
	); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *WriteEndpoint) Remove(relPath string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.RemoveReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgRemoveReq, Payload: payload},
	); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *WriteEndpoint) RemoveAll(relPath string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.RemoveAllReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgRemoveAllReq, Payload: payload},
	); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *WriteEndpoint) Symlink(target, newRel string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.SymlinkReq{Target: target, NewRel: serverPath(e.pathPrefix, newRel)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgSymlinkReq, Payload: payload},
	); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *WriteEndpoint) Link(oldRel, newRel string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.LinkReq{
		OldRel: serverPath(e.pathPrefix, oldRel),
		NewRel: serverPath(e.pathPrefix, newRel),
	}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgLinkReq, Payload: payload},
	); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *WriteEndpoint) SetMetadata(
	relPath string,
	entry transport.FileEntry,
	opts transport.MetadataOpts,
) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.SetMetadataReq{
		RelPath: serverPath(e.pathPrefix, relPath),
		Entry:   proto.FromFileEntry(entry),
		Opts:    proto.FromMetadataOpts(opts),
	}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgSetMetadataReq, Payload: payload},
	); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *WriteEndpoint) Walk(fn func(entry transport.FileEntry) error) error {
	return e.walkRel("", fn)
}

// WalkSubtree walks a subtree starting at subDir (relative to endpoint root).
// This is a beam-specific method (not on the transport.WriteEndpoint interface),
// accessed via type assertion for building destination indexes efficiently.
func (e *WriteEndpoint) WalkSubtree(subDir string, fn func(entry transport.FileEntry) error) error {
	return e.walkRel(subDir, fn)
}

// walkRel walks a subtree starting at walkRoot (relative to endpoint root).
// An empty walkRoot walks the entire endpoint root.
func (e *WriteEndpoint) walkRel(walkRoot string, fn func(entry transport.FileEntry) error) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.WalkReq{RelPath: serverPath(e.pathPrefix, walkRoot)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgWalkReq, Payload: payload},
	); err != nil {
		return err
	}

	// The server returns entries with RelPath relative to the walk root.
	for f := range ch {
		switch f.MsgType {
		case proto.MsgWalkEntry:
			var entry proto.WalkEntry
			if _, err := entry.UnmarshalMsg(f.Payload); err != nil {
				return err
			}
			if err := fn(proto.ToFileEntry(entry.Entry)); err != nil {
				return err
			}
		case proto.MsgWalkEnd:
			return nil
		case proto.MsgErrorResp:
			return decodeError(f.Payload)
		default:
			return fmt.Errorf("unexpected message type 0x%02x during walk", f.MsgType)
		}
	}
	return errors.New("stream closed during walk")
}

func (e *WriteEndpoint) Stat(relPath string) (transport.FileEntry, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.StatReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return transport.FileEntry{}, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgStatReq, Payload: payload},
	); err != nil {
		return transport.FileEntry{}, err
	}

	f, ok := <-ch
	if !ok {
		return transport.FileEntry{}, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return transport.FileEntry{}, decodeError(f.Payload)
	}

	var resp proto.StatResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return transport.FileEntry{}, err
	}
	entry := proto.ToFileEntry(resp.Entry)
	entry.RelPath = relPath // restore client-relative path
	return entry, nil
}

func (e *WriteEndpoint) OpenRead(relPath string) (io.ReadCloser, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)

	req := proto.OpenReadReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgOpenReadReq, Payload: payload},
	); err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}

	return &beamReader{mux: e.mux, streamID: streamID, ch: ch}, nil
}

func (e *WriteEndpoint) Hash(relPath string) (string, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.HashReq{RelPath: serverPath(e.pathPrefix, relPath)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return "", err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgHashReq, Payload: payload},
	); err != nil {
		return "", err
	}

	f, ok := <-ch
	if !ok {
		return "", errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return "", decodeError(f.Payload)
	}

	var resp proto.HashResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return "", err
	}
	return resp.Hash, nil
}

// ComputeSignature asks the remote daemon to compute block signatures of a file.
func (e *WriteEndpoint) ComputeSignature(
	relPath string,
	fileSize int64,
) (transport.Signature, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.ComputeSignatureReq{RelPath: serverPath(e.pathPrefix, relPath), FileSize: fileSize}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return transport.Signature{}, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgComputeSignatureReq, Payload: payload},
	); err != nil {
		return transport.Signature{}, err
	}

	f, ok := <-ch
	if !ok {
		return transport.Signature{}, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return transport.Signature{}, decodeError(f.Payload)
	}
	var resp proto.ComputeSignatureResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return transport.Signature{}, err
	}
	return proto.ToSignature(resp.BlockSize, resp.Signatures), nil
}

// ApplyDelta asks the remote daemon to reconstruct a file from basis + delta ops.
func (e *WriteEndpoint) ApplyDelta(
	basisRelPath, tempRelPath string,
	ops []transport.DeltaOp,
) (int64, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.ApplyDeltaReq{
		BasisRelPath: serverPath(e.pathPrefix, basisRelPath),
		TempRelPath:  serverPath(e.pathPrefix, tempRelPath),
		Ops:          proto.FromDeltaOps(ops),
	}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return 0, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgApplyDeltaReq, Payload: payload},
	); err != nil {
		return 0, err
	}

	f, ok := <-ch
	if !ok {
		return 0, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return 0, decodeError(f.Payload)
	}
	var resp proto.ApplyDeltaResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return 0, err
	}
	return resp.BytesWritten, nil
}

// WriteFileBatch sends a batch of small files to the remote daemon in a single
// round-trip. This is a beam-specific method (not on the transport.WriteEndpoint
// interface), accessed via type assertion, same pattern as delta transfer.
func (e *WriteEndpoint) WriteFileBatch(
	req proto.WriteFileBatchReq,
) ([]proto.WriteFileBatchResult, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	// Translate entry RelPaths to server-relative.
	for i := range req.Entries {
		req.Entries[i].RelPath = serverPath(e.pathPrefix, req.Entries[i].RelPath)
	}

	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	if err := e.mux.Send(
		proto.Frame{StreamID: streamID, MsgType: proto.MsgWriteFileBatchReq, Payload: payload},
	); err != nil {
		return nil, err
	}

	f, ok := <-ch
	if !ok {
		return nil, errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return nil, decodeError(f.Payload)
	}

	var resp proto.WriteFileBatchResp
	if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

func (e *WriteEndpoint) Root() string                 { return e.root }
func (e *WriteEndpoint) Caps() transport.Capabilities { return e.caps }
func (e *WriteEndpoint) Close() error                 { return e.mux.Close() }

func (*WriteEndpoint) expectAck(ch <-chan proto.Frame) error {
	f, ok := <-ch
	if !ok {
		return errors.New("stream closed")
	}
	if f.MsgType == proto.MsgErrorResp {
		return decodeError(f.Payload)
	}
	if f.MsgType != proto.MsgAckResp {
		return fmt.Errorf("expected ack, got 0x%02x", f.MsgType)
	}
	return nil
}

// beamWriteFile implements transport.WriteFile over the protocol.
// All WriteData and WriteDone frames are sent on the same stream that was
// used for CreateTemp so the server handler processes them sequentially.
type beamWriteFile struct {
	mux      *proto.Mux
	ch       <-chan proto.Frame
	handle   string
	name     string
	streamID uint32
}

func (f *beamWriteFile) Name() string { return f.name }

func (f *beamWriteFile) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > proto.DataChunkSize {
			chunk = chunk[:proto.DataChunkSize]
		}

		msg := proto.WriteDataMsg{Handle: f.handle, Data: chunk}
		payload, err := msg.MarshalMsg(nil)
		if err != nil {
			return total, err
		}
		if err := f.mux.Send(
			proto.Frame{StreamID: f.streamID, MsgType: proto.MsgWriteData, Payload: payload},
		); err != nil {
			return total, err
		}

		total += len(chunk)
		p = p[len(chunk):]
	}
	return total, nil
}

func (f *beamWriteFile) Close() error {
	defer f.mux.CloseStream(f.streamID)

	req := proto.WriteDoneReq{Handle: f.handle}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := f.mux.Send(
		proto.Frame{StreamID: f.streamID, MsgType: proto.MsgWriteDoneReq, Payload: payload},
	); err != nil {
		return err
	}

	resp, ok := <-f.ch
	if !ok {
		return errors.New("stream closed")
	}
	if resp.MsgType == proto.MsgErrorResp {
		return decodeError(resp.Payload)
	}
	return nil
}
