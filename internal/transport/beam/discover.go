package beam

import (
	"crypto/tls"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/config"
	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/proto"
)

// ReadRemoteDaemonDiscovery reads the daemon discovery file from a remote host
// over SFTP. Returns os.ErrNotExist if the daemon is not running (no discovery
// file). The caller's sshClient is NOT closed.
func ReadRemoteDaemonDiscovery(sshClient *ssh.Client) (config.DaemonDiscovery, error) {
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return config.DaemonDiscovery{}, fmt.Errorf("sftp client for discovery: %w", err)
	}
	defer sftpClient.Close()

	f, err := sftpClient.Open(config.SystemDaemonDiscoveryPath)
	if err != nil {
		if os.IsNotExist(err) {
			return config.DaemonDiscovery{}, os.ErrNotExist
		}
		return config.DaemonDiscovery{}, fmt.Errorf("open discovery file: %w", err)
	}
	defer f.Close()

	var d config.DaemonDiscovery
	if _, err := toml.NewDecoder(f).Decode(&d); err != nil {
		return config.DaemonDiscovery{}, fmt.Errorf("decode discovery file: %w", err)
	}
	return d, nil
}

// DialBeamTunnel establishes a beam protocol connection tunneled through an
// existing SSH connection. It dials the daemon on localhost:<port> via SSH TCP
// forwarding, wraps with TLS (self-signed), and performs the beam handshake.
//
//nolint:revive // function-result-limit: matches DialBeam/DialBeamConn signature
func DialBeamTunnel(
	sshClient *ssh.Client, discovery config.DaemonDiscovery,
) (*proto.Mux, string, transport.Capabilities, error) {
	// Tunnel TCP through SSH to the daemon's localhost port.
	addr := fmt.Sprintf("localhost:%d", discovery.Port)
	tunnelConn, err := sshClient.Dial("tcp", addr)
	if err != nil {
		return nil, "", transport.Capabilities{},
			fmt.Errorf("ssh tunnel to daemon port %d: %w", discovery.Port, err)
	}

	// Wrap with TLS (self-signed cert, so skip verify).
	tlsConn := tls.Client(tunnelConn, proto.ClientTLSConfig(true))
	if err := tlsConn.Handshake(); err != nil {
		tunnelConn.Close()
		return nil, "", transport.Capabilities{}, fmt.Errorf("tls handshake over tunnel: %w", err)
	}

	// Beam protocol handshake.
	return DialBeamConn(tlsConn, discovery.Token)
}

// TryBeamSSHRead attempts to discover a beam daemon on the remote host and
// connect via SSH tunnel. Returns a Reader if successful, or an error
// if no daemon is found or connection fails.
func TryBeamSSHRead(sshClient *ssh.Client, path string) (*Reader, error) {
	discovery, err := ReadRemoteDaemonDiscovery(sshClient)
	if err != nil {
		return nil, err
	}

	mux, root, caps, err := DialBeamTunnel(sshClient, discovery)
	if err != nil {
		return nil, err
	}

	return NewReader(mux, path, root, caps), nil
}

// TryBeamSSHWrite attempts to discover a beam daemon on the remote host and
// connect via SSH tunnel. Returns a Writer if successful, or an error
// if no daemon is found or connection fails.
func TryBeamSSHWrite(sshClient *ssh.Client, path string) (*Writer, error) {
	discovery, err := ReadRemoteDaemonDiscovery(sshClient)
	if err != nil {
		return nil, err
	}

	mux, root, caps, err := DialBeamTunnel(sshClient, discovery)
	if err != nil {
		return nil, err
	}

	return NewWriter(mux, path, root, caps), nil
}
