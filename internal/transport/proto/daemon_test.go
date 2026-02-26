package proto_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/transport/proto"
)

func testAuthOpts(t *testing.T) proto.AuthOpts {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	require.NoError(t, err)
	return proto.AuthOpts{
		Username: "testuser",
		Signer:   signer,
	}
}

func TestDaemonAuthSuccess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	authOpts := testAuthOpts(t)

	daemon, err := proto.NewDaemon(proto.DaemonConfig{
		ListenAddr: "127.0.0.1:0",
		Root:       dir,
		KeyChecker: func(_ string, _ ssh.PublicKey) bool { return true },
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go daemon.Serve(ctx) //nolint:errcheck // test daemon; error not needed

	// Connect as client.
	addr := daemon.Addr().String()
	rawConn, err := tls.Dial("tcp", addr, proto.ClientTLSConfig())
	require.NoError(t, err)
	defer rawConn.Close()

	conn, err := proto.NegotiateCompression(rawConn, false)
	require.NoError(t, err)

	mux := proto.NewMux(conn)
	go mux.Run() //nolint:errcheck // mux.Run error propagated via mux closure

	// Perform pubkey auth.
	root, authErr := proto.ClientAuth(mux, authOpts)
	require.NoError(t, authErr)
	assert.Equal(t, dir, root)

	mux.Close()
	cancel()
}

func TestDaemonAuthRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	authOpts := testAuthOpts(t)

	daemon, err := proto.NewDaemon(proto.DaemonConfig{
		ListenAddr: "127.0.0.1:0",
		Root:       dir,
		KeyChecker: func(_ string, _ ssh.PublicKey) bool { return false },
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go daemon.Serve(ctx) //nolint:errcheck // test daemon; error not needed

	// Connect as client.
	addr := daemon.Addr().String()
	rawConn, err := tls.Dial("tcp", addr, proto.ClientTLSConfig())
	require.NoError(t, err)
	defer rawConn.Close()

	conn, err := proto.NegotiateCompression(rawConn, false)
	require.NoError(t, err)

	mux := proto.NewMux(conn)
	go mux.Run() //nolint:errcheck // mux.Run error propagated via mux closure

	// Perform pubkey auth — should fail with a descriptive rejection reason,
	// not "timeout waiting for auth challenge" (which would indicate the
	// AuthResult frame was lost due to premature connection close).
	_, authErr := proto.ClientAuth(mux, authOpts)
	require.Error(t, authErr, "expected auth rejection")
	assert.Contains(t, authErr.Error(), "public key not authorized",
		"client should receive the actual rejection reason")
	assert.NotContains(t, authErr.Error(), "timeout",
		"should not be a timeout — the AuthResult frame must be delivered")

	mux.Close()
	cancel()
}

func TestDaemonDefaultConfig(t *testing.T) {
	t.Parallel()

	// Empty config should succeed — daemon defaults to root=/ and generates a cert.
	d, err := proto.NewDaemon(proto.DaemonConfig{ListenAddr: "127.0.0.1:0"})
	require.NoError(t, err)

	// Should have a valid fingerprint.
	assert.NotEmpty(t, d.Fingerprint())
	assert.Contains(t, d.Fingerprint(), "SHA256:")
}
