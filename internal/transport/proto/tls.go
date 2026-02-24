package proto

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// DefaultCertPath is the default path for the daemon TLS certificate.
	DefaultCertPath = "/etc/beam/daemon.crt"
	// DefaultKeyPath is the default path for the daemon TLS private key.
	DefaultKeyPath = "/etc/beam/daemon.key"
)

// GenerateSelfSignedCert creates a self-signed TLS certificate using P-256 ECDSA.
// The certificate is valid for 10 years and includes localhost and 127.0.0.1 as SANs.
func GenerateSelfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "beam daemon"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return tls.X509KeyPair(certPEM, keyPEM)
}

// LoadOrGenerateCert loads a TLS cert/key from disk, or generates and persists
// a new self-signed pair if they don't exist. Returns the certificate and its
// SHA256 fingerprint.
func LoadOrGenerateCert(certPath, keyPath string) (tls.Certificate, string, error) {
	if certPath == "" {
		certPath = DefaultCertPath
	}
	if keyPath == "" {
		keyPath = DefaultKeyPath
	}

	// Try loading existing cert.
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err == nil {
		fp, fpErr := CertFingerprint(cert)
		if fpErr != nil {
			return tls.Certificate{}, "", fpErr
		}
		return cert, fp, nil
	}

	// Generate new cert.
	cert, err = GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, "", fmt.Errorf("generate cert: %w", err)
	}

	// Persist to disk.
	if persistErr := persistCert(cert, certPath, keyPath); persistErr != nil {
		return tls.Certificate{}, "", fmt.Errorf("persist cert: %w", persistErr)
	}

	fp, err := CertFingerprint(cert)
	if err != nil {
		return tls.Certificate{}, "", err
	}
	return cert, fp, nil
}

func persistCert(cert tls.Certificate, certPath, keyPath string) error {
	if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
		return err
	}

	// Write certificate.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw})
	//nolint:gosec // G306: TLS cert is public data; only key needs restricted perms
	if writeErr := os.WriteFile(certPath, certPEM, 0o644); writeErr != nil {
		return fmt.Errorf("write cert: %w", writeErr)
	}

	// Write private key.
	ecKey, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return errors.New("expected ECDSA private key")
	}
	keyDER, err := x509.MarshalECPrivateKey(ecKey)
	if err != nil {
		return err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}

	return nil
}

// CertFingerprint returns the SHA256 fingerprint of a TLS certificate
// in the format "SHA256:<base64>".
func CertFingerprint(cert tls.Certificate) (string, error) {
	if len(cert.Certificate) == 0 {
		return "", errors.New("no certificate data")
	}
	h := sha256.Sum256(cert.Certificate[0])
	return "SHA256:" + base64.StdEncoding.EncodeToString(h[:]), nil
}

// CertFingerprintFromConn extracts the SHA256 fingerprint from a TLS
// connection's peer certificate.
func CertFingerprintFromConn(conn *tls.Conn) (string, error) {
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return "", errors.New("no peer certificates")
	}
	h := sha256.Sum256(state.PeerCertificates[0].Raw)
	return "SHA256:" + base64.StdEncoding.EncodeToString(h[:]), nil
}

// ClientTLSConfig returns a TLS config for connecting to a beam daemon.
// The config skips Go's built-in verification (self-signed) but the caller
// must verify the fingerprint after the handshake via VerifyFingerprint or
// TOFU.
func ClientTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // fingerprint verified after handshake
	}
}

// VerifyFingerprint checks that the TLS connection's peer cert matches the
// expected fingerprint. Returns nil on match.
func VerifyFingerprint(conn *tls.Conn, expected string) error {
	got, err := CertFingerprintFromConn(conn)
	if err != nil {
		return err
	}
	if got != expected {
		return fmt.Errorf(
			"TLS fingerprint mismatch: expected %s, got %s",
			expected, got,
		)
	}
	return nil
}

// KnownHosts manages a TOFU (trust-on-first-use) store for beam daemon
// TLS certificate fingerprints. Format: one "host fingerprint" per line.
type KnownHosts struct {
	entries map[string]string // host → fingerprint
	path    string
}

// DefaultKnownHostsPath returns ~/.config/beam/known_hosts.
func DefaultKnownHostsPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "beam", "known_hosts")
}

// LoadKnownHosts reads the known_hosts file. Returns an empty store if the
// file doesn't exist.
func LoadKnownHosts(path string) (*KnownHosts, error) {
	if path == "" {
		path = DefaultKnownHostsPath()
	}
	kh := &KnownHosts{path: path, entries: make(map[string]string)}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return kh, nil
		}
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 2 {
			kh.entries[parts[0]] = parts[1]
		}
	}
	return kh, scanner.Err()
}

// Verify checks the fingerprint for a host. Returns nil if the fingerprint
// matches or if the host is new (TOFU — the fingerprint is recorded).
// Returns an error if the host is known but the fingerprint changed.
func (kh *KnownHosts) Verify(host, fingerprint string) error {
	if existing, ok := kh.entries[host]; ok {
		if existing != fingerprint {
			return fmt.Errorf(
				"REMOTE HOST IDENTIFICATION HAS CHANGED for %s\n"+
					"Expected: %s\n"+
					"Got:      %s\n"+
					"This could indicate a man-in-the-middle attack.\n"+
					"Remove the entry from %s to accept the new key",
				host, existing, fingerprint, kh.path,
			)
		}
		return nil // known and matches
	}

	// TOFU: first time seeing this host, record it.
	kh.entries[host] = fingerprint
	return kh.save()
}

func (kh *KnownHosts) save() error {
	if err := os.MkdirAll(filepath.Dir(kh.path), 0o700); err != nil {
		return err
	}

	var b strings.Builder
	for host, fp := range kh.entries {
		fmt.Fprintf(&b, "%s %s\n", host, fp)
	}
	return os.WriteFile(kh.path, []byte(b.String()), 0o600)
}
