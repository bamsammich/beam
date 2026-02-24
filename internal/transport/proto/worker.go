package proto

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/bamsammich/beam/internal/transport"
)

// WorkerModeFlag is the hidden CLI flag that signals a child process should
// run as a beam protocol worker.
const WorkerModeFlag = "--worker-mode"

// WorkerModeFDEnv is the env var containing the inherited connection file
// descriptor number (always 3 when passed via ExtraFiles[0]).
const WorkerModeFDEnv = "BEAM_WORKER_FD"

// WorkerModeRootEnv is the env var containing the daemon root path.
const WorkerModeRootEnv = "BEAM_WORKER_ROOT"

// extractRawConn gets the underlying *net.TCPConn from a possibly-wrapped
// connection (e.g. *tls.Conn wrapping a TCP conn).
func extractRawConn(conn net.Conn) (*net.TCPConn, error) {
	// Try direct TCP.
	if tcp, ok := conn.(*net.TCPConn); ok {
		return tcp, nil
	}

	// TLS wraps the underlying conn — use NetConn() added in Go 1.18.
	if tlsConn, ok := conn.(*tls.Conn); ok {
		return extractRawConn(tlsConn.NetConn())
	}

	return nil, fmt.Errorf("cannot extract TCP conn from %T", conn)
}

// RunWorker is the entry point for a forked worker child process. It inherits
// the TLS connection fd from the parent, creates local endpoints as the
// authenticated user, and runs the beam mux+handler.
func RunWorker(ctx context.Context) error {
	fdStr := os.Getenv(WorkerModeFDEnv)
	root := os.Getenv(WorkerModeRootEnv)

	if fdStr == "" || root == "" {
		return fmt.Errorf(
			"worker mode requires %s and %s env vars", WorkerModeFDEnv, WorkerModeRootEnv,
		)
	}

	fd, err := strconv.Atoi(fdStr)
	if err != nil {
		return fmt.Errorf("invalid fd %q: %w", fdStr, err)
	}

	// Recover the connection from the inherited fd.
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

	// The connection is already TLS-wrapped by the parent. We use it as-is.
	conn := rawConn

	slog.Info("worker started", //nolint:gosec // G706: env vars are from trusted parent process
		"pid", os.Getpid(),
		"uid", os.Getuid(),
		"gid", os.Getgid(),
		"root", root,
	)

	// Create local endpoints rooted at the daemon root, running as the
	// authenticated user (kernel enforces permissions).
	readEP := transport.NewLocalReader(root)
	writeEP := transport.NewLocalWriter(root)

	mux := NewMux(conn)
	handler := NewHandler(readEP, writeEP, mux)

	mux.SetHandler(func(streamID uint32, ch <-chan Frame) {
		handler.ServeStream(streamID, ch)
		mux.CloseStream(streamID)
	})

	var muxWg sync.WaitGroup
	muxWg.Add(1)
	go func() {
		defer muxWg.Done()
		mux.Run() //nolint:errcheck // mux error propagated via closure
	}()

	select {
	case <-mux.Done():
	case <-ctx.Done():
		mux.Close()
	}
	muxWg.Wait()

	slog.Info("worker exiting", "pid", os.Getpid())
	return nil
}
