// Package beam implements ReadEndpoint and WriteEndpoint over the beam binary protocol.
package beam

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/proto"
)

// DefaultPort is the default beam daemon port.
const DefaultPort = 7223

// DialBeam connects to a beam daemon, performs TLS handshake and protocol
// handshake. Returns a running mux, the server root, and capabilities.
func DialBeam(addr, token string, tlsConfig *tls.Config) (*proto.Mux, string, transport.Capabilities, error) {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, tlsConfig)
	if err != nil {
		return nil, "", transport.Capabilities{}, fmt.Errorf("dial %s: %w", addr, err)
	}

	mux := proto.NewMux(conn)

	// Start mux in background.
	go mux.Run()

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
			return nil, "", transport.Capabilities{}, errors.New("connection closed during handshake")
		}
		if f.MsgType == proto.MsgErrorResp {
			var errResp proto.ErrorResp
			errResp.UnmarshalMsg(f.Payload) //nolint:errcheck // best effort
			mux.Close()
			return nil, "", transport.Capabilities{}, fmt.Errorf("handshake rejected: %s", errResp.Message)
		}
		if f.MsgType == 0 && len(f.Payload) == 0 {
			// Zero-value frame from closed channel.
			mux.Close()
			return nil, "", transport.Capabilities{}, errors.New("connection closed during handshake (auth rejected)")
		}
		if f.MsgType != proto.MsgHandshakeResp {
			mux.Close()
			return nil, "", transport.Capabilities{}, fmt.Errorf("unexpected message type 0x%02x during handshake", f.MsgType)
		}

		var resp proto.HandshakeResp
		if _, err := resp.UnmarshalMsg(f.Payload); err != nil {
			mux.Close()
			return nil, "", transport.Capabilities{}, fmt.Errorf("decode handshake response: %w", err)
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

	if err := mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgCapsReq, Payload: payload}); err != nil {
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
		return fmt.Errorf("protocol error (could not decode)")
	}
	return errors.New(errResp.Message)
}

// BeamReadEndpoint implements transport.ReadEndpoint over the beam protocol.
type BeamReadEndpoint struct {
	mux        *proto.Mux
	root       string
	caps       transport.Capabilities
	nextStream atomic.Uint32
}

// Compile-time interface check.
var _ transport.ReadEndpoint = (*BeamReadEndpoint)(nil)

// NewBeamReadEndpoint creates a read endpoint from an established mux connection.
func NewBeamReadEndpoint(mux *proto.Mux, root string, caps transport.Capabilities) *BeamReadEndpoint {
	ep := &BeamReadEndpoint{mux: mux, root: root, caps: caps}
	ep.nextStream.Store(2) // 1 was used for caps query
	return ep
}

func (e *BeamReadEndpoint) allocStream() uint32 {
	return e.nextStream.Add(1)
}

func (e *BeamReadEndpoint) Walk(fn func(entry transport.FileEntry) error) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.WalkReq{}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgWalkReq, Payload: payload}); err != nil {
		return err
	}

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

func (e *BeamReadEndpoint) Stat(relPath string) (transport.FileEntry, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.StatReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return transport.FileEntry{}, err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgStatReq, Payload: payload}); err != nil {
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
	return proto.ToFileEntry(resp.Entry), nil
}

func (e *BeamReadEndpoint) ReadDir(relPath string) ([]transport.FileEntry, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.ReadDirReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgReadDirReq, Payload: payload}); err != nil {
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

func (e *BeamReadEndpoint) OpenRead(relPath string) (io.ReadCloser, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)

	req := proto.OpenReadReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgOpenReadReq, Payload: payload}); err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}

	return &beamReader{mux: e.mux, streamID: streamID, ch: ch}, nil
}

func (e *BeamReadEndpoint) Hash(relPath string) (string, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.HashReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return "", err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgHashReq, Payload: payload}); err != nil {
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

func (e *BeamReadEndpoint) Root() string                { return e.root }
func (e *BeamReadEndpoint) Caps() transport.Capabilities { return e.caps }
func (e *BeamReadEndpoint) Close() error                 { return e.mux.Close() }

// beamReader implements io.ReadCloser by reading streamed data from the protocol.
type beamReader struct {
	mux      *proto.Mux
	streamID uint32
	ch       <-chan proto.Frame
	buf      []byte // unconsumed data from last ReadDataMsg
	done     bool
}

func (r *beamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}

		f, ok := <-r.ch
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
	}

	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *beamReader) Close() error {
	r.mux.CloseStream(r.streamID)
	return nil
}

// BeamWriteEndpoint implements transport.WriteEndpoint over the beam protocol.
type BeamWriteEndpoint struct {
	mux        *proto.Mux
	root       string
	caps       transport.Capabilities
	nextStream atomic.Uint32
}

// Compile-time interface check.
var _ transport.WriteEndpoint = (*BeamWriteEndpoint)(nil)

// NewBeamWriteEndpoint creates a write endpoint from an established mux connection.
func NewBeamWriteEndpoint(mux *proto.Mux, root string, caps transport.Capabilities) *BeamWriteEndpoint {
	ep := &BeamWriteEndpoint{mux: mux, root: root, caps: caps}
	ep.nextStream.Store(2)
	return ep
}

func (e *BeamWriteEndpoint) allocStream() uint32 {
	return e.nextStream.Add(1)
}

func (e *BeamWriteEndpoint) MkdirAll(relPath string, perm os.FileMode) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.MkdirAllReq{RelPath: relPath, Perm: uint32(perm)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgMkdirAllReq, Payload: payload}); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *BeamWriteEndpoint) CreateTemp(relPath string, perm os.FileMode) (transport.WriteFile, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)

	req := proto.CreateTempReq{RelPath: relPath, Perm: uint32(perm)}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgCreateTempReq, Payload: payload}); err != nil {
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
	return &beamWriteFile{
		mux:      e.mux,
		streamID: streamID,
		ch:       ch,
		handle:   resp.Handle,
		name:     resp.Name,
	}, nil
}

func (e *BeamWriteEndpoint) Rename(oldRel, newRel string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.RenameReq{OldRel: oldRel, NewRel: newRel}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgRenameReq, Payload: payload}); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *BeamWriteEndpoint) Remove(relPath string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.RemoveReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgRemoveReq, Payload: payload}); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *BeamWriteEndpoint) RemoveAll(relPath string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.RemoveAllReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgRemoveAllReq, Payload: payload}); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *BeamWriteEndpoint) Symlink(target, newRel string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.SymlinkReq{Target: target, NewRel: newRel}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgSymlinkReq, Payload: payload}); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *BeamWriteEndpoint) Link(oldRel, newRel string) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.LinkReq{OldRel: oldRel, NewRel: newRel}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgLinkReq, Payload: payload}); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *BeamWriteEndpoint) SetMetadata(relPath string, entry transport.FileEntry, opts transport.MetadataOpts) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.SetMetadataReq{
		RelPath: relPath,
		Entry:   proto.FromFileEntry(entry),
		Opts:    proto.FromMetadataOpts(opts),
	}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgSetMetadataReq, Payload: payload}); err != nil {
		return err
	}

	return e.expectAck(ch)
}

func (e *BeamWriteEndpoint) Walk(fn func(entry transport.FileEntry) error) error {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.WalkReq{}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgWalkReq, Payload: payload}); err != nil {
		return err
	}

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

func (e *BeamWriteEndpoint) Stat(relPath string) (transport.FileEntry, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.StatReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return transport.FileEntry{}, err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgStatReq, Payload: payload}); err != nil {
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
	return proto.ToFileEntry(resp.Entry), nil
}

func (e *BeamWriteEndpoint) OpenRead(relPath string) (io.ReadCloser, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)

	req := proto.OpenReadReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgOpenReadReq, Payload: payload}); err != nil {
		e.mux.CloseStream(streamID)
		return nil, err
	}

	return &beamReader{mux: e.mux, streamID: streamID, ch: ch}, nil
}

func (e *BeamWriteEndpoint) Hash(relPath string) (string, error) {
	streamID := e.allocStream()
	ch := e.mux.OpenStream(streamID)
	defer e.mux.CloseStream(streamID)

	req := proto.HashReq{RelPath: relPath}
	payload, err := req.MarshalMsg(nil)
	if err != nil {
		return "", err
	}
	if err := e.mux.Send(proto.Frame{StreamID: streamID, MsgType: proto.MsgHashReq, Payload: payload}); err != nil {
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

func (e *BeamWriteEndpoint) Root() string                { return e.root }
func (e *BeamWriteEndpoint) Caps() transport.Capabilities { return e.caps }
func (e *BeamWriteEndpoint) Close() error                 { return e.mux.Close() }

func (e *BeamWriteEndpoint) expectAck(ch <-chan proto.Frame) error {
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
	streamID uint32
	ch       <-chan proto.Frame
	handle   string
	name     string
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
		if err := f.mux.Send(proto.Frame{StreamID: f.streamID, MsgType: proto.MsgWriteData, Payload: payload}); err != nil {
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
	if err := f.mux.Send(proto.Frame{StreamID: f.streamID, MsgType: proto.MsgWriteDoneReq, Payload: payload}); err != nil {
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
