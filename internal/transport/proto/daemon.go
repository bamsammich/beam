package proto

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/transport"
)

// DaemonConfig configures a beam protocol daemon.
type DaemonConfig struct {
	TLSCert *tls.Certificate
	// KeyChecker overrides the default authorized_keys validation in
	// ServerAuth. If set, called instead of checking ~/.ssh/authorized_keys.
	// Intended for tests.
	KeyChecker  func(username string, pubkey ssh.PublicKey) bool
	ListenAddr  string
	Root        string
	TLSCertPath string // disk path for fork worker to load cert
	TLSKeyPath  string // disk path for fork worker to load key
	// ForkMode controls whether the daemon forks a child process per connection
	// to run as the authenticated user. When false (e.g. in tests or
	// single-user mode), the daemon handles connections in-process.
	ForkMode bool
}

// Daemon serves the beam protocol over TLS.
type Daemon struct {
	listener    net.Listener
	readEP      *transport.LocalReader
	writeEP     *transport.LocalWriter
	conns       map[net.Conn]struct{}
	auth        *ServerAuth
	fingerprint string
	cfg         DaemonConfig
	mu          sync.Mutex
}

// NewDaemon creates a new beam daemon. Call Serve to start accepting connections.
//
//nolint:revive // cognitive-complexity: fork vs in-process listener setup
func NewDaemon(cfg DaemonConfig) (*Daemon, error) {
	if cfg.Root == "" {
		cfg.Root = "/"
	}

	var tlsCert tls.Certificate
	var fingerprint string
	if cfg.TLSCert != nil {
		tlsCert = *cfg.TLSCert
		var err error
		fingerprint, err = CertFingerprint(tlsCert)
		if err != nil {
			return nil, fmt.Errorf("compute cert fingerprint: %w", err)
		}
	} else {
		var err error
		tlsCert, err = GenerateSelfSignedCert()
		if err != nil {
			return nil, fmt.Errorf("generate self-signed cert: %w", err)
		}
		fingerprint, err = CertFingerprint(tlsCert)
		if err != nil {
			return nil, fmt.Errorf("compute cert fingerprint: %w", err)
		}
		slog.Info("generated self-signed TLS certificate", "fingerprint", fingerprint)
	}

	var listener net.Listener
	if cfg.ForkMode {
		// Fork mode: accept raw TCP. The child process handles TLS after
		// fork so it owns the full TLS session state.
		var err error
		listener, err = net.Listen("tcp", cfg.ListenAddr)
		if err != nil {
			return nil, fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
		}
	} else {
		// In-process mode: TLS in the parent.
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			MinVersion:   tls.VersionTLS12,
		}
		var err error
		listener, err = tls.Listen("tcp", cfg.ListenAddr, tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
		}
	}

	auth := NewServerAuth(cfg.Root)
	auth.KeyChecker = cfg.KeyChecker

	return &Daemon{
		cfg:         cfg,
		listener:    listener,
		readEP:      transport.NewLocalReader(cfg.Root),
		writeEP:     transport.NewLocalWriter(cfg.Root),
		conns:       make(map[net.Conn]struct{}),
		auth:        auth,
		fingerprint: fingerprint,
	}, nil
}

// Addr returns the listener's address (useful when listening on :0).
func (d *Daemon) Addr() net.Addr {
	return d.listener.Addr()
}

// Fingerprint returns the TLS certificate fingerprint.
func (d *Daemon) Fingerprint() string {
	return d.fingerprint
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

	if d.cfg.ForkMode {
		// Fork mode: the connection is raw TCP. Fork immediately so the
		// child owns the full connection lifecycle (TLS + auth + handler).
		d.handleFork(conn)
		return
	}

	// In-process mode: conn is *tls.Conn from tls.Listen.
	conn, err := AcceptCompression(conn)
	if err != nil {
		slog.Warn("compression negotiation failed", "remote", remoteAddr, "error", err)
		return
	}

	mux := NewMux(conn)

	// Open control stream before Run so the reader doesn't discard auth frames.
	controlCh := mux.OpenStream(ControlStream)

	// Set handler before Run so the readLoop never discards frames for
	// unregistered streams. During auth the client only uses the control
	// stream (already registered above), so the handler won't fire until
	// the client sends post-auth requests (e.g. CapsReq).
	handler := NewHandler(d.readEP, d.writeEP, mux)
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

	// Authenticate via SSH pubkey challenge/response.
	username, err := d.auth.Authenticate(mux, controlCh)
	if err != nil {
		slog.Warn("authentication failed", "remote", remoteAddr, "error", err)
		// Shutdown drains pending writes (e.g. AuthResult{ok:false}) so
		// the client receives the rejection reason before disconnect.
		mux.Shutdown()
		muxWg.Wait()
		return
	}

	slog.Info("authenticated", "remote", remoteAddr, "username", username)

	d.handleInProcess(ctx, mux, &muxWg, remoteAddr)
}

func (d *Daemon) handleFork(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		slog.Error("fork mode requires TCP connection", "type", fmt.Sprintf("%T", conn))
		return
	}

	connFile, err := tcpConn.File()
	if err != nil {
		slog.Error("get connection file for fork", "error", err)
		return
	}
	defer connFile.Close()

	if forkErr := ForkWorker(
		connFile,
		d.cfg.Root,
		d.cfg.TLSCertPath,
		d.cfg.TLSKeyPath,
	); forkErr != nil {
		slog.Error("fork worker failed", "error", forkErr)
	}
}

func (*Daemon) handleInProcess(
	ctx context.Context, mux *Mux, muxWg *sync.WaitGroup, remoteAddr string,
) {
	// Wait for mux to finish (connection closed).
	select {
	case <-mux.Done():
	case <-ctx.Done():
		mux.Close()
	}
	muxWg.Wait()

	slog.Info("connection closed", "remote", remoteAddr)
}
