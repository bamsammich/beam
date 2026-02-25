//go:build unix

package proto

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
)

// ForkWorker re-execs the current binary as a child process. The raw TCP
// connection fd is passed via ExtraFiles (fd 3). The child starts as root,
// performs TLS handshake + auth, then drops privileges to the authenticated
// user via setuid/setgid.
func ForkWorker(connFile *os.File, root, tlsCertPath, tlsKeyPath string) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}

	cmd := exec.Command(executable, WorkerModeFlag)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{connFile} // fd 3 in child
	cmd.Env = append(os.Environ(),
		WorkerModeFDEnv+"=3",
		WorkerModeRootEnv+"="+root,
		WorkerModeTLSCertEnv+"="+tlsCertPath,
		WorkerModeTLSKeyEnv+"="+tlsKeyPath,
	)

	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setPdeathsig(cmd.SysProcAttr)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}

	slog.Info("forked worker", "pid", cmd.Process.Pid)

	// Detach — the parent closes the connection fd; the child owns it now.
	// The child process will be reaped by the Go runtime's SIGCHLD handler.
	go func() {
		if waitErr := cmd.Wait(); waitErr != nil {
			slog.Debug("worker exited", "pid", cmd.Process.Pid, "error", waitErr)
		}
	}()

	return nil
}

// dropPrivileges drops the process to the given UID/GID/groups.
// Must be called while still running as root. Order matters: supplementary
// groups and GID must be set before UID (can't change groups after losing root).
// Go 1.16+ propagates setuid/setgid to all OS threads via runtime_doAllThreadsSyscall.
func dropPrivileges(uid, gid uint32, groups []uint32) error {
	if err := syscall.Setgroups(convertGroups(groups)); err != nil {
		return fmt.Errorf("setgroups: %w", err)
	}
	if err := syscall.Setgid(int(gid)); err != nil { //nolint:gosec // G115: bounded by caller
		return fmt.Errorf("setgid(%d): %w", gid, err)
	}
	if err := syscall.Setuid(int(uid)); err != nil { //nolint:gosec // G115: bounded by caller
		return fmt.Errorf("setuid(%d): %w", uid, err)
	}
	return nil
}

// convertGroups converts []uint32 to []int for syscall.Setgroups.
func convertGroups(groups []uint32) []int {
	out := make([]int, len(groups))
	for i, g := range groups {
		out[i] = int(g) //nolint:gosec // G115: bounded by caller
	}
	return out
}
