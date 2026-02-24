package proto

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os/user"
	"strconv"
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
	KeyChecker func(username string, pubkey ssh.PublicKey) bool
	ListenAddr string
	Root       string
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

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}

	listener, err := tls.Listen("tcp", cfg.ListenAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
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

//nolint:revive // cognitive-complexity: auth + fork/in-process dispatch
func (d *Daemon) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	slog.Info("new connection", "remote", remoteAddr)

	mux := NewMux(conn)

	// Open control stream before Run so the reader doesn't discard auth frames.
	controlCh := mux.OpenStream(ControlStream)

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
		mux.Close()
		muxWg.Wait()
		return
	}

	slog.Info("authenticated", "remote", remoteAddr, "username", username)

	if d.cfg.ForkMode {
		// Fork a child process running as the authenticated user.
		d.handleFork(conn, username, mux, &muxWg)
		return
	}

	// In-process mode (tests, single-user): run handler directly.
	d.handleInProcess(ctx, mux, &muxWg, remoteAddr)
}

func (d *Daemon) handleFork(conn net.Conn, username string, mux *Mux, muxWg *sync.WaitGroup) {
	// Look up OS user for credential info.
	u, err := user.Lookup(username)
	if err != nil {
		slog.Error("user lookup for fork", "username", username, "error", err)
		mux.Close()
		muxWg.Wait()
		return
	}

	uid, _ := strconv.ParseUint(u.Uid, 10, 32) //nolint:errcheck // validated by user.Lookup
	gid, _ := strconv.ParseUint(u.Gid, 10, 32) //nolint:errcheck // validated by user.Lookup

	groupIDs, _ := u.GroupIds() //nolint:errcheck // best-effort
	groups := make([]uint32, 0, len(groupIDs))
	for _, gidStr := range groupIDs {
		g, parseErr := strconv.ParseUint(gidStr, 10, 32)
		if parseErr == nil {
			groups = append(groups, uint32(g)) //nolint:gosec // G115: bounded by ParseUint
		}
	}

	// Close the mux in the parent — the child will create its own.
	mux.Close()
	muxWg.Wait()

	if forkErr := ForkWorker(
		conn,
		uint32(uid), //nolint:gosec // G115: bounded by ParseUint
		uint32(gid), //nolint:gosec // G115: bounded by ParseUint
		groups,
		d.cfg.Root,
	); forkErr != nil {
		slog.Error("fork worker failed", "username", username, "error", forkErr)
	}
}

func (d *Daemon) handleInProcess(
	ctx context.Context, mux *Mux, muxWg *sync.WaitGroup, remoteAddr string,
) {
	handler := NewHandler(d.readEP, d.writeEP, mux)

	mux.SetHandler(func(streamID uint32, ch <-chan Frame) {
		handler.ServeStream(streamID, ch)
		mux.CloseStream(streamID)
	})

	// Wait for mux to finish (connection closed).
	select {
	case <-mux.Done():
	case <-ctx.Done():
		mux.Close()
	}
	muxWg.Wait()

	slog.Info("connection closed", "remote", remoteAddr)
}
