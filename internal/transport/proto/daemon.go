package proto

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/bamsammich/beam/internal/transport"
)

// DaemonConfig configures a beam protocol daemon.
type DaemonConfig struct {
	TLSCert    *tls.Certificate
	ListenAddr string
	Root       string
	AuthToken  string //nolint:gosec // G117: field name is descriptive, not a credential leak
}

// Daemon serves the beam protocol over TLS.
type Daemon struct {
	listener net.Listener
	readEP   *transport.LocalReadEndpoint
	writeEP  *transport.LocalWriteEndpoint
	conns    map[net.Conn]struct{}
	cfg      DaemonConfig
	mu       sync.Mutex
}

// NewDaemon creates a new beam daemon. Call Serve to start accepting connections.
func NewDaemon(cfg DaemonConfig) (*Daemon, error) {
	if cfg.Root == "" {
		return nil, errors.New("daemon root directory is required")
	}
	if cfg.AuthToken == "" {
		return nil, errors.New("daemon auth token is required")
	}

	var tlsCert tls.Certificate
	if cfg.TLSCert != nil {
		tlsCert = *cfg.TLSCert
	} else {
		var err error
		tlsCert, err = GenerateSelfSignedCert()
		if err != nil {
			return nil, fmt.Errorf("generate self-signed cert: %w", err)
		}
		slog.Info("generated self-signed TLS certificate")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}

	listener, err := tls.Listen("tcp", cfg.ListenAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
	}

	return &Daemon{
		cfg:      cfg,
		listener: listener,
		readEP:   transport.NewLocalReadEndpoint(cfg.Root),
		writeEP:  transport.NewLocalWriteEndpoint(cfg.Root),
		conns:    make(map[net.Conn]struct{}),
	}, nil
}

// Addr returns the listener's address (useful when listening on :0).
func (d *Daemon) Addr() net.Addr {
	return d.listener.Addr()
}

// Serve accepts connections until ctx is cancelled. Blocks until shutdown completes.
func (d *Daemon) Serve(ctx context.Context) error {
	slog.Info("beam daemon listening", "addr", d.listener.Addr(), "root", d.cfg.Root)

	var wg sync.WaitGroup

	// Shutdown goroutine: when ctx is cancelled, stop the listener and drain connections.
	go func() {
		<-ctx.Done()
		d.listener.Close()

		// Give active connections 30s to finish.
		time.AfterFunc(30*time.Second, func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			for conn := range d.conns {
				conn.Close()
			}
		})
	}()

	for {
		conn, err := d.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // graceful shutdown
			}
			slog.Error("accept error", "error", err)
			continue
		}

		d.mu.Lock()
		d.conns[conn] = struct{}{}
		d.mu.Unlock()

		wg.Go(func() {
			defer func() {
				d.mu.Lock()
				delete(d.conns, conn)
				d.mu.Unlock()
			}()
			d.handleConn(ctx, conn)
		})
	}

	wg.Wait()
	return nil
}

func (d *Daemon) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	slog.Info("new connection", "remote", remoteAddr)

	mux := NewMux(conn)

	// Open control stream before Run so the reader doesn't discard handshake frames.
	controlCh := mux.OpenStream(ControlStream)

	// Create handler backed by local endpoints rooted at daemon root.
	handler := NewHandler(d.readEP, d.writeEP, mux)

	// Set handler for dynamically created streams. When a client sends a
	// frame on a new stream ID, the mux auto-creates the stream and calls
	// this handler in a new goroutine. The handler processes the single
	// request-response exchange and the stream is cleaned up by ServeStream.
	mux.SetHandler(func(streamID uint32, ch <-chan Frame) {
		handler.ServeStream(streamID, ch)
		mux.CloseStream(streamID)
	})

	var muxWg sync.WaitGroup
	muxWg.Add(1)
	go func() {
		defer muxWg.Done()
		mux.Run() //nolint:errcheck // mux.Run error propagated via mux closure and conn.Close
	}()

	// Wait for handshake on control stream.
	if !d.handleHandshake(mux, controlCh) {
		slog.Warn("handshake failed", "remote", remoteAddr)
		mux.Close()
		muxWg.Wait()
		return
	}

	slog.Info("authenticated", "remote", remoteAddr)

	// Wait for mux to finish (connection closed).
	select {
	case <-mux.Done():
	case <-ctx.Done():
		mux.Close()
	}
	muxWg.Wait()

	slog.Info("connection closed", "remote", remoteAddr)
}

func (d *Daemon) handleHandshake(mux *Mux, ch <-chan Frame) bool {
	// Wait for HandshakeReq with 10s timeout.
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	var f Frame
	select {
	case f = <-ch:
	case <-timer.C:
		return false
	}

	if f.MsgType != MsgHandshakeReq {
		return false
	}

	var req HandshakeReq
	if _, err := req.UnmarshalMsg(f.Payload); err != nil {
		return false
	}

	// Validate auth token.
	if req.AuthToken != d.cfg.AuthToken {
		resp := ErrorResp{Message: "authentication failed"}
		payload, _ := resp.MarshalMsg(nil) //nolint:errcheck // best-effort error response
		_ = mux.Send(                      //nolint:errcheck // best-effort error response
			Frame{StreamID: ControlStream, MsgType: MsgErrorResp, Payload: payload},
		)
		return false
	}

	// Send HandshakeResp.
	resp := HandshakeResp{
		Version: ProtocolVersion,
		Root:    d.cfg.Root,
	}
	payload, err := resp.MarshalMsg(nil)
	if err != nil {
		return false
	}
	if err := mux.Send(
		Frame{StreamID: ControlStream, MsgType: MsgHandshakeResp, Payload: payload},
	); err != nil {
		return false
	}

	return true
}
