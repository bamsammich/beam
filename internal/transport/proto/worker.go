package proto

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/user"
	"strconv"
	"sync"

	"github.com/bamsammich/beam/internal/transport"
)

// WorkerModeFlag is the hidden CLI flag that signals a child process should
// run as a beam protocol worker.
const WorkerModeFlag = "--worker-mode"

// Worker env var names.
const (
	// WorkerModeFDEnv is the env var containing the inherited connection file
	// descriptor number (always 3 when passed via ExtraFiles[0]).
	WorkerModeFDEnv = "BEAM_WORKER_FD"

	// WorkerModeRootEnv is the env var containing the daemon root path.
	WorkerModeRootEnv = "BEAM_WORKER_ROOT"

	// WorkerModeTLSCertEnv is the env var containing the TLS certificate path.
	WorkerModeTLSCertEnv = "BEAM_TLS_CERT"

	// WorkerModeTLSKeyEnv is the env var containing the TLS private key path.
	WorkerModeTLSKeyEnv = "BEAM_TLS_KEY"
)

// RunWorker is the entry point for a forked worker child process. It inherits
// a raw TCP connection fd from the parent, performs TLS handshake and auth,
// drops privileges to the authenticated user, then runs the beam mux+handler.
//
//nolint:gocyclo,revive // cyclomatic,cognitive-complexity: full connection lifecycle
func RunWorker(ctx context.Context) error {
	fdStr := os.Getenv(WorkerModeFDEnv)
	root := os.Getenv(WorkerModeRootEnv)
	tlsCertPath := os.Getenv(WorkerModeTLSCertEnv)
	tlsKeyPath := os.Getenv(WorkerModeTLSKeyEnv)

	if fdStr == "" || root == "" {
		return fmt.Errorf(
			"worker mode requires %s and %s env vars", WorkerModeFDEnv, WorkerModeRootEnv,
		)
	}
	if tlsCertPath == "" || tlsKeyPath == "" {
		return fmt.Errorf(
			"worker mode requires %s and %s env vars",
			WorkerModeTLSCertEnv, WorkerModeTLSKeyEnv,
		)
	}

	fd, err := strconv.Atoi(fdStr)
	if err != nil {
		return fmt.Errorf("invalid fd %q: %w", fdStr, err)
	}

	// Recover the raw TCP connection from the inherited fd.
	connFile := os.NewFile(uintptr(fd), "beam-conn") //nolint:gosec // fd from trusted parent
	if connFile == nil {
		return fmt.Errorf("invalid file descriptor %d", fd)
	}

	rawConn, err := net.FileConn(connFile)
	if err != nil {
		connFile.Close()
		return fmt.Errorf("recover connection from fd %d: %w", fd, err)
	}
	connFile.Close() // FileConn dups the fd; close our copy

	// Load TLS cert and perform handshake — the child owns the TLS session.
	tlsCert, err := tls.LoadX509KeyPair(tlsCertPath, tlsKeyPath)
	if err != nil {
		rawConn.Close()
		return fmt.Errorf("load TLS cert: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}
	tlsConn := tls.Server(rawConn, tlsConfig)
	if hsErr := tlsConn.Handshake(); hsErr != nil {
		tlsConn.Close()
		return fmt.Errorf("TLS handshake: %w", hsErr)
	}

	slog.Info("worker TLS handshake complete", //nolint:gosec // G706: env vars from trusted parent
		"pid", os.Getpid(),
		"root", root,
	)

	// Negotiate compression before starting the mux.
	muxConn, err := AcceptCompression(tlsConn)
	if err != nil {
		tlsConn.Close()
		return fmt.Errorf("compression negotiation: %w", err)
	}

	// Create mux and authenticate.
	mux := NewMux(muxConn)
	controlCh := mux.OpenStream(ControlStream)

	var muxWg sync.WaitGroup
	muxWg.Add(1)
	go func() {
		defer muxWg.Done()
		mux.Run() //nolint:errcheck // mux error propagated via closure
	}()

	auth := NewServerAuth(root)
	username, err := auth.Authenticate(mux, controlCh)
	if err != nil {
		slog.Warn("worker auth failed", "pid", os.Getpid(), "error", err)
		mux.Shutdown()
		muxWg.Wait()
		return fmt.Errorf("authenticate: %w", err)
	}

	slog.Info( //nolint:gosec // G706: username from auth, not user input
		"worker authenticated",
		"pid",
		os.Getpid(),
		"username",
		username,
	)

	// Look up OS user and drop privileges.
	u, err := user.Lookup(username)
	if err != nil {
		mux.Close()
		muxWg.Wait()
		return fmt.Errorf("user lookup %q: %w", username, err)
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

	if err := dropPrivileges(
		uint32(uid), //nolint:gosec // G115: bounded by ParseUint
		uint32(gid), //nolint:gosec // G115: bounded by ParseUint
		groups,
	); err != nil {
		mux.Close()
		muxWg.Wait()
		return fmt.Errorf("drop privileges: %w", err)
	}

	slog.Info("worker privileges dropped",
		"pid", os.Getpid(),
		"uid", os.Getuid(),
		"gid", os.Getgid(),
	)

	// Create local endpoints as the authenticated user.
	readEP := transport.NewLocalReader(root)
	writeEP := transport.NewLocalWriter(root)

	handler := NewHandler(readEP, writeEP, mux)

	mux.SetHandler(func(streamID uint32, ch <-chan Frame) {
		handler.ServeStream(streamID, ch)
		mux.CloseStream(streamID)
	})

	select {
	case <-mux.Done():
	case <-ctx.Done():
		mux.Close()
	}
	muxWg.Wait()

	slog.Info("worker exiting", "pid", os.Getpid())
	return nil
}
