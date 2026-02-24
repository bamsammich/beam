//go:build unix

package proto

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"syscall"
)

// ForkWorker re-execs the current binary as a child process running with the
// given UID/GID credentials. The TLS connection is passed to the child via
// ExtraFiles (fd 3). The child runs the beam mux/handler as the authenticated user.
func ForkWorker(conn net.Conn, uid, gid uint32, groups []uint32, root string) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}

	// Get the raw fd from the connection. For TLS connections we need the
	// underlying TCP connection.
	rawConn, err := extractRawConn(conn)
	if err != nil {
		return fmt.Errorf("extract raw connection: %w", err)
	}

	connFile, err := rawConn.File()
	if err != nil {
		return fmt.Errorf("get connection file: %w", err)
	}
	defer connFile.Close()

	cmd := exec.Command(executable, WorkerModeFlag)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{connFile} // fd 3 in child
	cmd.Env = append(os.Environ(),
		WorkerModeFDEnv+"=3",
		WorkerModeRootEnv+"="+root,
	)

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    uid,
			Gid:    gid,
			Groups: groups,
		},
	}
	setPdeathsig(cmd.SysProcAttr)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}

	slog.Info("forked worker", "pid", cmd.Process.Pid, "uid", uid, "gid", gid)

	// Detach — the parent closes the connection fd; the child owns it now.
	// The child process will be reaped by the Go runtime's SIGCHLD handler.
	go func() {
		if waitErr := cmd.Wait(); waitErr != nil {
			slog.Debug("worker exited", "pid", cmd.Process.Pid, "error", waitErr)
		}
	}()

	return nil
}
