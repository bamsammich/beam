// Package beam implements Reader and Writer over the beam binary protocol.
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

// Conn holds the results of a successful beam dial.
type Conn struct {
	Mux  *proto.Mux
	Root string
	Caps transport.Capabilities
}

// DialBeam connects to a beam daemon over TLS, performs SSH pubkey auth.
// Returns a Conn containing the running mux, server root, and capabilities.
func DialBeam(
	addr string,
	authOpts proto.AuthOpts,
	tlsConfig *tls.Config,
	compress bool,
) (Conn, error) {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, tlsConfig)
	if err != nil {
		return Conn{}, fmt.Errorf("dial %s: %w", addr, err)
	}

	// Verify TLS fingerprint if provided.
	if authOpts.Fingerprint != "" {
		if fpErr := proto.VerifyFingerprint(conn, authOpts.Fingerprint); fpErr != nil {
			conn.Close()
			return Conn{}, fpErr
		}
	}

	return DialConn(conn, authOpts, compress)
}

// DialConn performs beam SSH pubkey auth over an already-established
// connection. This is the core auth logic shared by DialBeam (direct TLS)
// and DialBeamTunnel (SSH-tunneled TLS). Returns a Conn containing the
// running mux, server root, and capabilities.
func DialConn(
	conn net.Conn, authOpts proto.AuthOpts, compress bool,
) (Conn, error) {
	muxConn, err := proto.NegotiateCompression(conn, compress)
	if err != nil {
		conn.Close()
		return Conn{}, fmt.Errorf("compression negotiation: %w", err)
	}

	mux := proto.NewMux(muxConn)

	// Start mux in background.
	go mux.Run() //nolint:errcheck // mux.Run error propagated via mux closure

	// Perform SSH pubkey authentication.
	root, err := proto.ClientAuth(mux, authOpts)
	if err != nil {
		mux.Close()
		return Conn{}, fmt.Errorf("auth: %w", err)
	}

	// Query capabilities.
	caps, err := queryCaps(mux)
	if err != nil {
		mux.Close()
		return Conn{}, fmt.Errorf("query capabilities: %w", err)
	}

	return Conn{Mux: mux, Root: root, Caps: caps}, nil
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

// Reader implements transport.Reader over the beam protocol.
type Reader struct {
	mux        *proto.Mux
	root       string // client-side root (loc.Path)
	pathPrefix string // prefix to prepend to relPaths for server resolution
	caps       transport.Capabilities
	nextStream atomic.Uint32
}

// Compile-time interface checks.
var (
	_ transport.Reader      = (*Reader)(nil)
	_ transport.DeltaSource = (*Reader)(nil)
)

// NewReader creates a read endpoint from an established mux connection.
// root is the client's target path (loc.Path), daemonRoot is the server's root.
// All relPaths sent to the server are prefixed with the path from daemonRoot to root.
func NewReader(
	mux *proto.Mux,
	root, daemonRoot string,
	caps transport.Capabilities,
) *Reader {
	prefix := computePathPrefix(root, daemonRoot)
	ep := &Reader{mux: mux, root: root, pathPrefix: prefix, caps: caps}
	ep.nextStream.Store(2) // 1 was used for caps query
	return ep
}

func (e *Reader) allocStream() uint32 {
	return e.nextStream.Add(1)
}

func (e *Reader) Walk(fn func(entry transport.FileEntry) error) error {
	return e.walkRel("", fn)
}

// walkRel walks a subtree starting at walkRoot (relative to endpoint root).
// An empty walkRoot walks the entire endpoint root.
func (e *Reader) walkRel(walkRoot string, fn func(entry transport.FileEntry) error) error {
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

func (e *Reader) Stat(relPath string) (transport.FileEntry, error) {
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

func (e *Reader) ReadDir(relPath string) ([]transport.FileEntry, error) {
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

func (e *Reader) OpenRead(relPath string) (io.ReadCloser, error) {
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

func (e *Reader) Hash(relPath string) (string, error) {
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
func (e *Reader) ComputeSignature(
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
func (e *Reader) MatchBlocks(
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

func (e *Reader) Root() string                 { return e.root }
func (e *Reader) Caps() transport.Capabilities { return e.caps }
func (e *Reader) Close() error                 { return e.mux.Close() }

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

// Writer implements transport.ReadWriter over the beam protocol.
type Writer struct {
	mux        *proto.Mux
	root       string // client-side root (loc.Path)
	pathPrefix string // prefix to prepend to relPaths for server resolution
	caps       transport.Capabilities
	nextStream atomic.Uint32
}

// Compile-time interface checks.
var (
	_ transport.ReadWriter    = (*Writer)(nil)
	_ transport.BatchWriter   = (*Writer)(nil)
	_ transport.DeltaTarget   = (*Writer)(nil)
	_ transport.SubtreeWalker = (*Writer)(nil)
)

// NewWriter creates a write endpoint from an established mux connection.
// root is the client's target path (loc.Path), daemonRoot is the server's root.
func NewWriter(
	mux *proto.Mux,
	root, daemonRoot string,
	caps transport.Capabilities,
) *Writer {
	prefix := computePathPrefix(root, daemonRoot)
	ep := &Writer{mux: mux, root: root, pathPrefix: prefix, caps: caps}
	ep.nextStream.Store(2)
	return ep
}

func (e *Writer) allocStream() uint32 {
	return e.nextStream.Add(1)
}

func (e *Writer) MkdirAll(relPath string, perm os.FileMode) error {
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

//nolint:ireturn // implements transport.ReadWriter interface; WriteFile is the standard return type
func (e *Writer) CreateTemp(
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

func (e *Writer) Rename(oldRel, newRel string) error {
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

func (e *Writer) Remove(relPath string) error {
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

func (e *Writer) RemoveAll(relPath string) error {
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

func (e *Writer) Symlink(target, newRel string) error {
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

func (e *Writer) Link(oldRel, newRel string) error {
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

func (e *Writer) SetMetadata(
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

func (e *Writer) Walk(fn func(entry transport.FileEntry) error) error {
	return e.walkRel("", fn)
}

// WalkSubtree walks a subtree starting at subDir (relative to endpoint root).
// This is a beam-specific method (not on the transport.ReadWriter interface),
// accessed via type assertion for building destination indexes efficiently.
func (e *Writer) WalkSubtree(subDir string, fn func(entry transport.FileEntry) error) error {
	return e.walkRel(subDir, fn)
}

// walkRel walks a subtree starting at walkRoot (relative to endpoint root).
// An empty walkRoot walks the entire endpoint root.
func (e *Writer) walkRel(walkRoot string, fn func(entry transport.FileEntry) error) error {
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

func (e *Writer) Stat(relPath string) (transport.FileEntry, error) {
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

func (e *Writer) OpenRead(relPath string) (io.ReadCloser, error) {
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

func (e *Writer) Hash(relPath string) (string, error) {
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
func (e *Writer) ComputeSignature(
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
func (e *Writer) ApplyDelta(
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
// round-trip. Implements transport.BatchWriter.
func (e *Writer) WriteFileBatch(
	req transport.BatchWriteRequest,
) ([]transport.BatchWriteResult, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	wireReq := proto.FromBatchWriteRequest(req)

	// Translate entry RelPaths to server-relative.
	for i := range wireReq.Entries {
		wireReq.Entries[i].RelPath = serverPath(e.pathPrefix, wireReq.Entries[i].RelPath)
	}

	payload, err := wireReq.MarshalMsg(nil)
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
	return proto.ToBatchWriteResults(resp.Results), nil
}

func (e *Writer) Root() string                 { return e.root }
func (e *Writer) Caps() transport.Capabilities { return e.caps }
func (e *Writer) Close() error                 { return e.mux.Close() }

func (*Writer) expectAck(ch <-chan proto.Frame) error {
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
