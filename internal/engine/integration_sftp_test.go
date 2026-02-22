//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/engine"
	"github.com/bamsammich/beam/internal/transport"
)

// startSFTPContainer starts an atmoz/sftp container with the given directory
// bind-mounted at /home/testuser/data. Returns host and port for SSH.
func startSFTPContainer(t *testing.T, bindMountDir string) (host string, port int) {
	t.Helper()
	ctx := context.Background()

	// Use the host user's uid/gid so files written via SFTP are owned by the
	// test process, allowing t.TempDir() cleanup to delete them.
	uid := os.Getuid()
	gid := os.Getgid()
	userSpec := fmt.Sprintf("testuser:testpass:%d:%d:data", uid, gid)

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "atmoz/sftp:latest",
			ExposedPorts: []string{"22/tcp"},
			Cmd:          []string{userSpec},
			Mounts: testcontainers.Mounts(
				testcontainers.BindMount(bindMountDir, "/home/testuser/data"),
			),
			WaitingFor: wait.ForListeningPort("22/tcp").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	}

	ctr, err := testcontainers.GenericContainer(ctx, req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	h, err := ctr.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(ctx, "22/tcp")
	require.NoError(t, err)

	p, err := strconv.Atoi(mappedPort.Port())
	require.NoError(t, err)

	return h, p
}

// dialTestSSH connects to an SSH server with password auth and retry logic.
// Does NOT register cleanup — the SFTP endpoint's Close() handles that.
func dialTestSSH(t *testing.T, host string, port int) *ssh.Client {
	t.Helper()

	config := &ssh.ClientConfig{
		User:            "testuser",
		Auth:            []ssh.AuthMethod{ssh.Password("testpass")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", host, port)

	var client *ssh.Client
	var err error
	for range 10 {
		client, err = ssh.Dial("tcp", addr, config)
		if err == nil {
			return client
		}
		time.Sleep(500 * time.Millisecond)
	}

	require.NoError(t, err, "failed to connect to SFTP container at %s after retries", addr)
	return nil
}

func TestIntegration_LocalToSFTP(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createTestTree(t, srcDir)

	// chmod 0777 so container user (uid 1001) can write.
	require.NoError(t, os.Chmod(dstDir, 0o777))

	host, port := startSFTPContainer(t, dstDir)

	sshClient := dialTestSSH(t, host, port)
	dstEP, err := transport.NewSFTPWriteEndpoint(sshClient, "/data")
	require.NoError(t, err)
	t.Cleanup(func() { dstEP.Close() })

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		DstEndpoint: dstEP,
	})

	require.NoError(t, result.Err)
	assert.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

	verifyTreeCopy(t, srcDir, dstDir)
}

func TestIntegration_SFTPToLocal(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	createTestTree(t, srcDir)

	// chmod 0777 so container can read the bind-mounted source.
	require.NoError(t, os.Chmod(srcDir, 0o777))

	host, port := startSFTPContainer(t, srcDir)

	sshClient := dialTestSSH(t, host, port)
	sftpReadEP, err := transport.NewSFTPReadEndpoint(sshClient, "/data")
	require.NoError(t, err)
	t.Cleanup(func() { sftpReadEP.Close() })

	// Wrap to override Root() so filepath.Rel(ep.Root(), task.SrcPath) works.
	// The scanner walks the local srcDir, producing absolute local paths.
	// The worker computes relSrc = filepath.Rel(SrcEndpoint.Root(), srcPath)
	// and then reads via SrcEndpoint.OpenRead(relSrc). The re-rooted wrapper
	// makes Root() return srcDir so the relative path is correct for the
	// SFTP endpoint whose root is /data (the chroot-relative mount point).
	srcEP := &reRootedReadEndpoint{
		ReadEndpoint: sftpReadEP,
		localRoot:    srcDir,
	}

	result := engine.Run(context.Background(), engine.Config{
		Sources:     []string{srcDir + "/"},
		Dst:         dstDir,
		Archive:     true,
		Recursive:   true,
		Workers:     2,
		Events:      drainEvents(t),
		SrcEndpoint: srcEP,
	})

	require.NoError(t, result.Err)
	assert.GreaterOrEqual(t, result.Stats.FilesCopied, int64(4))

	verifyTreeCopy(t, srcDir, dstDir)
}
