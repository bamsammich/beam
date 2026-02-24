package proto_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/bamsammich/beam/internal/transport/proto"
)

// generateKeyPair produces a fresh ECDSA key pair and returns an ssh.Signer + ssh.PublicKey.
//
//nolint:ireturn // ssh.Signer is the standard interface for SSH signers
func generateKeyPair(t *testing.T) (ssh.Signer, ssh.PublicKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	require.NoError(t, err)
	return signer, signer.PublicKey()
}

// startTestDaemonForAuth creates a daemon with the given KeyChecker and returns
// its address and fingerprint.
func startTestDaemonForAuth(
	t *testing.T,
	keyChecker func(string, ssh.PublicKey) bool,
) (addr string, fingerprint string, cancel context.CancelFunc) {
	t.Helper()

	dir := t.TempDir()
	daemon, err := proto.NewDaemon(proto.DaemonConfig{
		ListenAddr: "127.0.0.1:0",
		Root:       dir,
		KeyChecker: keyChecker,
	})
	require.NoError(t, err)

	ctx, cancelFn := context.WithCancel(context.Background())
	go daemon.Serve(ctx) //nolint:errcheck // test daemon; error not needed

	return daemon.Addr().String(), daemon.Fingerprint(), cancelFn
}

func TestAuthWrongKeyRejected(t *testing.T) {
	t.Parallel()

	// Generate two separate key pairs.
	signerA, _ := generateKeyPair(t)
	_, pubkeyB := generateKeyPair(t)

	// Daemon authorizes pubkeyB only.
	addr, _, cancel := startTestDaemonForAuth(t, func(_ string, pk ssh.PublicKey) bool {
		return ssh.FingerprintSHA256(pk) == ssh.FingerprintSHA256(pubkeyB)
	})
	defer cancel()

	// Client presents pubkeyA (not authorized) with signerA.
	conn, err := tls.Dial("tcp", addr, proto.ClientTLSConfig())
	require.NoError(t, err)
	defer conn.Close()

	mux := proto.NewMux(conn)
	go mux.Run() //nolint:errcheck // test mux; error propagated via closure

	_, authErr := proto.ClientAuth(mux, proto.AuthOpts{
		Username: "testuser",
		Signer:   signerA,
	})
	require.Error(t, authErr, "should reject unauthorized key")
	// The server sends AuthResult{ok:false} and closes the mux.
	// The client may see "authentication failed: public key not authorized"
	// or a timeout/connection close depending on timing.

	mux.Close()
}

func TestAuthWrongSignatureRejected(t *testing.T) {
	t.Parallel()

	// Generate two key pairs. The daemon authorizes pubkeyA.
	// We'll craft a signer that presents pubkeyA but signs with keyB.
	signerA, pubkeyA := generateKeyPair(t)
	signerB, _ := generateKeyPair(t)

	// Daemon checks pubkeyA is authorized.
	addr, _, cancel := startTestDaemonForAuth(t, func(_ string, pk ssh.PublicKey) bool {
		return ssh.FingerprintSHA256(pk) == ssh.FingerprintSHA256(pubkeyA)
	})
	defer cancel()

	// Create a mismatched signer: presents A's public key, signs with B's private key.
	wrongSigner := &mismatchedSigner{
		pubkey: signerA.PublicKey(),
		signer: signerB,
	}

	conn, err := tls.Dial("tcp", addr, proto.ClientTLSConfig())
	require.NoError(t, err)
	defer conn.Close()

	mux := proto.NewMux(conn)
	go mux.Run() //nolint:errcheck // test mux; error propagated via closure

	_, authErr := proto.ClientAuth(mux, proto.AuthOpts{
		Username: "testuser",
		Signer:   wrongSigner,
	})
	require.Error(t, authErr, "should reject mismatched signature")
	// The server sends AuthResult{ok:false} and closes the mux.
	// The client may see "signature verification failed" or a
	// timeout/connection close depending on timing.

	mux.Close()
}

// mismatchedSigner presents one public key but signs with a different private key.
type mismatchedSigner struct {
	pubkey ssh.PublicKey
	signer ssh.Signer
}

//nolint:ireturn // implementing ssh.Signer interface
func (m *mismatchedSigner) PublicKey() ssh.PublicKey {
	return m.pubkey
}

func (m *mismatchedSigner) Sign(rand2 io.Reader, data []byte) (*ssh.Signature, error) {
	return m.signer.Sign(rand2, data)
}

func TestAuthConcurrentClients(t *testing.T) {
	t.Parallel()

	// Accept all keys.
	addr, _, cancel := startTestDaemonForAuth(t, func(_ string, _ ssh.PublicKey) bool {
		return true
	})
	defer cancel()

	const numClients = 10

	// Pre-generate key pairs on the test goroutine (require is not goroutine-safe).
	signers := make([]ssh.Signer, numClients)
	for i := range numClients {
		signers[i], _ = generateKeyPair(t)
	}

	var wg sync.WaitGroup
	errs := make([]error, numClients)

	for i := range numClients {
		wg.Go(func() {
			conn, dialErr := tls.Dial("tcp", addr, proto.ClientTLSConfig())
			if dialErr != nil {
				errs[i] = dialErr
				return
			}
			defer conn.Close()

			mux := proto.NewMux(conn)
			go mux.Run() //nolint:errcheck // test mux; error propagated via closure

			_, authErr := proto.ClientAuth(mux, proto.AuthOpts{
				Username: "testuser",
				Signer:   signers[i],
			})
			errs[i] = authErr

			mux.Close()
		})
	}

	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "client %d should authenticate successfully", i)
	}
}

func TestAuthSuccessReturnsRoot(t *testing.T) {
	t.Parallel()

	signer, _ := generateKeyPair(t)

	dir := t.TempDir()
	daemon, err := proto.NewDaemon(proto.DaemonConfig{
		ListenAddr: "127.0.0.1:0",
		Root:       dir,
		KeyChecker: func(_ string, _ ssh.PublicKey) bool { return true },
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go daemon.Serve(ctx) //nolint:errcheck // test daemon; error not needed

	conn, err := tls.Dial("tcp", daemon.Addr().String(), proto.ClientTLSConfig())
	require.NoError(t, err)
	defer conn.Close()

	mux := proto.NewMux(conn)
	go mux.Run() //nolint:errcheck // test mux; error propagated via closure

	root, authErr := proto.ClientAuth(mux, proto.AuthOpts{
		Username: "testuser",
		Signer:   signer,
	})
	require.NoError(t, authErr)
	assert.Equal(t, dir, root, "auth result should return daemon root")

	mux.Close()
}

func TestAuthUsernamePassedToChecker(t *testing.T) {
	t.Parallel()

	signer, _ := generateKeyPair(t)

	var capturedUsername string
	addr, _, cancel := startTestDaemonForAuth(t, func(username string, _ ssh.PublicKey) bool {
		capturedUsername = username
		return true
	})
	defer cancel()

	conn, err := tls.Dial("tcp", addr, proto.ClientTLSConfig())
	require.NoError(t, err)
	defer conn.Close()

	mux := proto.NewMux(conn)
	go mux.Run() //nolint:errcheck // test mux; error propagated via closure

	_, authErr := proto.ClientAuth(mux, proto.AuthOpts{
		Username: "alice",
		Signer:   signer,
	})
	require.NoError(t, authErr)
	assert.Equal(t, "alice", capturedUsername)

	mux.Close()
}

func TestTLSFingerprintVerification(t *testing.T) {
	t.Parallel()

	addr, correctFP, cancel := startTestDaemonForAuth(t, func(_ string, _ ssh.PublicKey) bool {
		return true
	})
	defer cancel()

	// Correct fingerprint — should succeed.
	conn, err := tls.Dial("tcp", addr, proto.ClientTLSConfig())
	require.NoError(t, err)

	verifyErr := proto.VerifyFingerprint(conn, correctFP)
	require.NoError(t, verifyErr, "correct fingerprint should verify")
	conn.Close()

	// Wrong fingerprint — should fail.
	conn2, err := tls.Dial("tcp", addr, proto.ClientTLSConfig())
	require.NoError(t, err)

	verifyErr = proto.VerifyFingerprint(conn2, "SHA256:AAAAAAAAAAAAAAAAAAAAAAAA")
	require.Error(t, verifyErr, "wrong fingerprint should fail verification")
	assert.Contains(t, verifyErr.Error(), "fingerprint mismatch")
	conn2.Close()
}
