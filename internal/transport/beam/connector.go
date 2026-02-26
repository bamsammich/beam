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
	_ transport.Transport = (*Transport)(nil)
	_ transport.Transport = (*SSHTransport)(nil)
)

// Transport manages a direct beam:// protocol connection.
// Endpoints are cached: the first ReaderAt/ReadWriterAt creates and stores
// the endpoint; subsequent calls return the cached one. This is necessary
// because beam endpoints share a mux with sequentially-allocated stream IDs.
type Transport struct {
	tlsConfig *tls.Config
	authOpts  proto.AuthOpts
	mux       *proto.Mux
	readEP    *Reader
	writeEP   *Writer
	addr      string
	root      string
	caps      transport.Capabilities
	connected bool
	compress  bool
}

// NewTransport creates a transport for a direct beam:// URL.
// Connection is lazy — the first ReaderAt or ReadWriterAt triggers DialBeam.
func NewTransport(
	addr string,
	authOpts proto.AuthOpts,
	tlsConfig *tls.Config,
	compress bool,
) *Transport {
	return &Transport{
		addr:      addr,
		authOpts:  authOpts,
		tlsConfig: tlsConfig,
		compress:  compress,
	}
}

// NewTransportFromMux creates a transport from an already-established mux
// (e.g. from an SSH tunnel). The connection is already active.
func NewTransportFromMux(mux *proto.Mux, root string, caps transport.Capabilities) *Transport {
	return &Transport{
		mux:       mux,
		root:      root,
		caps:      caps,
		connected: true,
	}
}

func (c *Transport) connect() error {
	if c.connected {
		return nil
	}
	bc, err := DialBeam(c.addr, c.authOpts, c.tlsConfig, c.compress)
	if err != nil {
		return err
	}
	c.mux = bc.Mux
	c.root = bc.Root
	c.caps = bc.Caps
	c.connected = true
	return nil
}

//nolint:ireturn // implements Transport interface
func (c *Transport) ReaderAt(path string) (transport.Reader, error) {
	if c.readEP != nil {
		return c.readEP, nil
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	c.readEP = NewReader(c.mux, path, c.root, c.caps)
	return c.readEP, nil
}

//nolint:ireturn // implements Transport interface
func (c *Transport) ReadWriterAt(path string) (transport.ReadWriter, error) {
	if c.writeEP != nil {
		return c.writeEP, nil
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	c.writeEP = NewWriter(c.mux, path, c.root, c.caps)
	return c.writeEP, nil
}

func (*Transport) Protocol() transport.Protocol { return transport.ProtocolBeam }

func (c *Transport) Close() error {
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

// SSHTransportOpts configures an SSH transport.
type SSHTransportOpts struct {
	AuthOpts proto.AuthOpts
	Host     string
	User     string
	SSHOpts  transport.SSHOpts
	NoBeam   bool
	Compress bool
}

// SSHTransport handles SSH with beam auto-detection.
// It dials SSH, checks for a beam daemon, and delegates to either a
// Transport (via tunnel) or an sftpTransport.
type SSHTransport struct {
	inner     transport.Transport
	sshClient *ssh.Client
	opts      SSHTransportOpts
}

// NewSSHTransport creates an SSH transport. Connection is lazy.
func NewSSHTransport(opts SSHTransportOpts) *SSHTransport {
	return &SSHTransport{opts: opts}
}

//nolint:revive // cognitive-complexity: SSH dial + beam detection + fallback
func (c *SSHTransport) ensureConnected() error {
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
			bc, tunnelErr := DialBeamTunnel(
				sshClient,
				discovery,
				c.opts.AuthOpts,
				c.opts.Compress,
			)
			if tunnelErr == nil {
				slog.Info("using beam protocol (daemon detected on remote)")
				c.inner = NewTransportFromMux(bc.Mux, bc.Root, bc.Caps)
				return nil
			}
			slog.Debug("beam tunnel failed, using SFTP", "error", tunnelErr)
		} else {
			slog.Debug("beam daemon not detected, using SFTP", "error", discErr)
		}
	}

	c.inner = &sftpTransport{sshClient: sshClient}
	return nil
}

//nolint:ireturn // implements Transport interface
func (c *SSHTransport) ReaderAt(path string) (transport.Reader, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}
	return c.inner.ReaderAt(path)
}

//nolint:ireturn // implements Transport interface
func (c *SSHTransport) ReadWriterAt(path string) (transport.ReadWriter, error) {
	if err := c.ensureConnected(); err != nil {
		return nil, err
	}
	return c.inner.ReadWriterAt(path)
}

func (c *SSHTransport) Protocol() transport.Protocol {
	if c.inner != nil {
		return c.inner.Protocol()
	}
	return transport.ProtocolSFTP
}

func (c *SSHTransport) Close() error {
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

// sftpTransport creates SFTP endpoints using a borrowed SSH connection.
type sftpTransport struct {
	sshClient *ssh.Client
}

//nolint:ireturn // implements Transport interface
func (c *sftpTransport) ReaderAt(path string) (transport.Reader, error) {
	return transport.NewSFTPReaderBorrowed(c.sshClient, path)
}

//nolint:ireturn // implements Transport interface
func (c *sftpTransport) ReadWriterAt(path string) (transport.ReadWriter, error) {
	return transport.NewSFTPWriterBorrowed(c.sshClient, path)
}

func (*sftpTransport) Protocol() transport.Protocol { return transport.ProtocolSFTP }
func (*sftpTransport) Close() error                 { return nil }
