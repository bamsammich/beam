package beam

import (
	"crypto/tls"
	"fmt"
	"log/slog"

	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/proto"
)

// Compile-time interface checks.
var (
	_ transport.Connector = (*Connector)(nil)
	_ transport.Connector = (*SSHConnector)(nil)
)

// Connector manages a direct beam:// protocol connection.
// Endpoints are cached: the first ConnectRead/ConnectWrite creates and stores
// the endpoint; subsequent calls return the cached one. This is necessary
// because beam endpoints share a mux with sequentially-allocated stream IDs.
type Connector struct {
	tlsConfig *tls.Config
	mux       *proto.Mux
	readEP    *ReadEndpoint
	writeEP   *WriteEndpoint
	addr      string
	token     string
	root      string
	caps      transport.Capabilities
	connected bool
}

// NewConnector creates a connector for a direct beam:// URL.
// Connection is lazy — the first ConnectRead or ConnectWrite triggers DialBeam.
func NewConnector(addr, token string, tlsConfig *tls.Config) *Connector {
	return &Connector{
		addr:      addr,
		token:     token,
		tlsConfig: tlsConfig,
	}
}

// NewConnectorFromMux creates a connector from an already-established mux
// (e.g. from an SSH tunnel). The connection is already active.
func NewConnectorFromMux(mux *proto.Mux, root string, caps transport.Capabilities) *Connector {
	return &Connector{
		mux:       mux,
		root:      root,
		caps:      caps,
		connected: true,
	}
}

func (c *Connector) connect() error {
	if c.connected {
		return nil
	}
	mux, root, caps, err := DialBeam(c.addr, c.token, c.tlsConfig)
	if err != nil {
		return err
	}
	c.mux = mux
	c.root = root
	c.caps = caps
	c.connected = true
	return nil
}

//nolint:ireturn // implements Connector interface
func (c *Connector) ConnectRead(path string) (transport.ReadEndpoint, error) {
	if c.readEP != nil {
		return c.readEP, nil
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	c.readEP = NewReadEndpoint(c.mux, path, c.root, c.caps)
	return c.readEP, nil
}

//nolint:ireturn // implements Connector interface
func (c *Connector) ConnectWrite(path string) (transport.WriteEndpoint, error) {
	if c.writeEP != nil {
		return c.writeEP, nil
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	c.writeEP = NewWriteEndpoint(c.mux, path, c.root, c.caps)
	return c.writeEP, nil
}

func (*Connector) Protocol() transport.Protocol { return transport.ProtocolBeam }

func (c *Connector) Close() error {
	if c.readEP != nil {
		c.readEP.Close()
	}
	if c.writeEP != nil {
		c.writeEP.Close()
	}
	if c.mux != nil {
		return c.mux.Close()
	}
	return nil
}

// SSHConnectorOpts configures an SSH connector.
type SSHConnectorOpts struct {
	Host    string
	User    string
	SSHOpts transport.SSHOpts
	NoBeam  bool
}

// SSHConnector handles SSH with beam auto-detection.
// It dials SSH, checks for a beam daemon, and delegates to either a
// Connector (via tunnel) or an sftpConnector.
type SSHConnector struct {
	inner     transport.Connector
	sshClient *ssh.Client
	opts      SSHConnectorOpts
}

// NewSSHConnector creates an SSH connector. Connection is lazy.
func NewSSHConnector(opts SSHConnectorOpts) *SSHConnector {
	return &SSHConnector{opts: opts}
}

//nolint:revive // cognitive-complexity: SSH dial + beam detection + fallback
func (c *SSHConnector) ensureConnected() error {
	if c.inner != nil {
		return nil
	}

	sshClient, err := transport.DialSSH(c.opts.Host, c.opts.User, c.opts.SSHOpts)
	if err != nil {
		return fmt.Errorf("ssh connect to %s: %w", c.opts.Host, err)
	}
	c.sshClient = sshClient

	if !c.opts.NoBeam {
		discovery, discErr := ReadRemoteDaemonDiscovery(sshClient)
		if discErr == nil {
			mux, root, caps, tunnelErr := DialBeamTunnel(sshClient, discovery)
			if tunnelErr == nil {
				slog.Info("using beam protocol (daemon detected on remote)")
				c.inner = NewConnectorFromMux(mux, root, caps)
				return nil
			}
			slog.Debug("beam tunnel failed, using SFTP", "error", tunnelErr)
		} else {
			slog.Debug("beam daemon not detected, using SFTP", "error", discErr)
		}
	}

	c.inner = &sftpConnector{sshClient: sshClient}
	return nil
}

//nolint:ireturn // implements Connector interface
func (c *SSHConnector) ConnectRead(path string) (transport.ReadEndpoint, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}
	return c.inner.ConnectRead(path)
}

//nolint:ireturn // implements Connector interface
func (c *SSHConnector) ConnectWrite(path string) (transport.WriteEndpoint, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}
	return c.inner.ConnectWrite(path)
}

func (c *SSHConnector) Protocol() transport.Protocol {
	if c.inner != nil {
		return c.inner.Protocol()
	}
	return transport.ProtocolSFTP
}

func (c *SSHConnector) Close() error {
	var err error
	if c.inner != nil {
		err = c.inner.Close()
	}
	if c.sshClient != nil {
		if sshErr := c.sshClient.Close(); sshErr != nil && err == nil {
			err = sshErr
		}
	}
	return err
}

// sftpConnector creates SFTP endpoints using a borrowed SSH connection.
type sftpConnector struct {
	sshClient *ssh.Client
}

//nolint:ireturn // implements Connector interface
func (c *sftpConnector) ConnectRead(path string) (transport.ReadEndpoint, error) {
	return transport.NewSFTPReadEndpointBorrowed(c.sshClient, path)
}

//nolint:ireturn // implements Connector interface
func (c *sftpConnector) ConnectWrite(path string) (transport.WriteEndpoint, error) {
	return transport.NewSFTPWriteEndpointBorrowed(c.sshClient, path)
}

func (*sftpConnector) Protocol() transport.Protocol { return transport.ProtocolSFTP }
func (*sftpConnector) Close() error                 { return nil }
