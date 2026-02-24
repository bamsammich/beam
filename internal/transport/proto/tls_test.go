package proto_test

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/transport/proto"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	t.Parallel()

	cert, err := proto.GenerateSelfSignedCert()
	require.NoError(t, err)
	require.NotEmpty(t, cert.Certificate)
	require.NotNil(t, cert.PrivateKey)
}

func TestCertFingerprint(t *testing.T) {
	t.Parallel()

	cert, err := proto.GenerateSelfSignedCert()
	require.NoError(t, err)

	fp, err := proto.CertFingerprint(cert)
	require.NoError(t, err)
	assert.Contains(t, fp, "SHA256:")

	// Same cert should produce the same fingerprint.
	fp2, err := proto.CertFingerprint(cert)
	require.NoError(t, err)
	assert.Equal(t, fp, fp2)
}

func TestCertFingerprintDifferentCerts(t *testing.T) {
	t.Parallel()

	cert1, err := proto.GenerateSelfSignedCert()
	require.NoError(t, err)
	cert2, err := proto.GenerateSelfSignedCert()
	require.NoError(t, err)

	fp1, err := proto.CertFingerprint(cert1)
	require.NoError(t, err)
	fp2, err := proto.CertFingerprint(cert2)
	require.NoError(t, err)

	assert.NotEqual(t, fp1, fp2, "different certs should have different fingerprints")
}

func TestLoadOrGenerateCert_Generates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certPath := filepath.Join(dir, "daemon.crt")
	keyPath := filepath.Join(dir, "daemon.key")

	cert, fp, err := proto.LoadOrGenerateCert(certPath, keyPath)
	require.NoError(t, err)
	assert.NotEmpty(t, cert.Certificate)
	assert.Contains(t, fp, "SHA256:")

	// Files should exist on disk.
	_, err = os.Stat(certPath)
	require.NoError(t, err)
	_, err = os.Stat(keyPath)
	require.NoError(t, err)

	// Key file should have restricted permissions.
	keyInfo, err := os.Stat(keyPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), keyInfo.Mode().Perm())
}

func TestLoadOrGenerateCert_Persists(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certPath := filepath.Join(dir, "daemon.crt")
	keyPath := filepath.Join(dir, "daemon.key")

	// Generate.
	_, fp1, err := proto.LoadOrGenerateCert(certPath, keyPath)
	require.NoError(t, err)

	// Load — should return same cert (same fingerprint).
	_, fp2, err := proto.LoadOrGenerateCert(certPath, keyPath)
	require.NoError(t, err)

	assert.Equal(t, fp1, fp2, "reloaded cert should have same fingerprint")
}

func TestVerifyFingerprint(t *testing.T) {
	t.Parallel()

	// Start a TLS server.
	cert, err := proto.GenerateSelfSignedCert()
	require.NoError(t, err)

	fp, err := proto.CertFingerprint(cert)
	require.NoError(t, err)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	require.NoError(t, err)
	defer listener.Close()

	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		// Keep connection alive for the test.
		buf := make([]byte, 1)
		conn.Read(buf) //nolint:errcheck // test server: just hold connection open
	}()

	// Connect as client.
	conn, err := tls.Dial("tcp", listener.Addr().String(), proto.ClientTLSConfig())
	require.NoError(t, err)
	defer conn.Close()

	// Correct fingerprint should match.
	require.NoError(t, proto.VerifyFingerprint(conn, fp))

	// Wrong fingerprint should fail.
	err = proto.VerifyFingerprint(conn, "SHA256:wrong")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fingerprint mismatch")
}

func TestKnownHosts_TOFU(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "known_hosts")

	kh, err := proto.LoadKnownHosts(path)
	require.NoError(t, err)

	// First time: should accept and record (TOFU).
	require.NoError(t, kh.Verify("example.com:9876", "SHA256:abc123"))

	// File should exist.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(data), "example.com:9876 SHA256:abc123")

	// Same fingerprint: should still pass.
	kh2, err := proto.LoadKnownHosts(path)
	require.NoError(t, err)
	require.NoError(t, kh2.Verify("example.com:9876", "SHA256:abc123"))

	// Different fingerprint: should fail (MITM warning).
	err = kh2.Verify("example.com:9876", "SHA256:different")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REMOTE HOST IDENTIFICATION HAS CHANGED")
}

func TestKnownHosts_MultipleHosts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "known_hosts")

	kh, err := proto.LoadKnownHosts(path)
	require.NoError(t, err)

	require.NoError(t, kh.Verify("host1:9876", "SHA256:aaa"))
	require.NoError(t, kh.Verify("host2:9876", "SHA256:bbb"))

	// Reload and verify both persist.
	kh2, err := proto.LoadKnownHosts(path)
	require.NoError(t, err)
	require.NoError(t, kh2.Verify("host1:9876", "SHA256:aaa"))
	require.NoError(t, kh2.Verify("host2:9876", "SHA256:bbb"))
}

func TestKnownHosts_EmptyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent", "known_hosts")

	// Should create the file and directory on first use.
	kh, err := proto.LoadKnownHosts(path)
	require.NoError(t, err)
	require.NoError(t, kh.Verify("newhost:9876", "SHA256:xxx"))

	_, err = os.Stat(path)
	require.NoError(t, err)
}
