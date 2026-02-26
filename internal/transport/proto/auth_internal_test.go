package proto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

//nolint:ireturn // ssh.Signer is the standard interface for SSH signers
func testKeyPair(t *testing.T) (ssh.Signer, ssh.PublicKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	require.NoError(t, err)
	return signer, signer.PublicKey()
}

// TestAuthorizedKeysPaths is not parallel because subtests mutate the
// package-level _sshdPath variable.
func TestAuthorizedKeysPaths(t *testing.T) {
	t.Run("parses sshd output", func(t *testing.T) {
		dir := t.TempDir()
		script := filepath.Join(dir, "fake-sshd")
		err := os.WriteFile(script, []byte(
			"#!/bin/sh\necho 'authorizedkeysfile .ssh/authorized_keys /etc/ssh/authorized_keys.d/%u'\n",
		), 0o755)
		require.NoError(t, err)

		old := _sshdPath
		_sshdPath = script
		t.Cleanup(func() { _sshdPath = old })

		paths := authorizedKeysPaths("alice")
		assert.Equal(t, []string{".ssh/authorized_keys", "/etc/ssh/authorized_keys.d/%u"}, paths)
	})

	t.Run("falls back when sshd not found", func(t *testing.T) {
		old := _sshdPath
		_sshdPath = "/nonexistent/sshd"
		t.Cleanup(func() { _sshdPath = old })

		paths := authorizedKeysPaths("bob")
		assert.Equal(t, []string{".ssh/authorized_keys"}, paths)
	})

	t.Run("falls back on sshd failure", func(t *testing.T) {
		dir := t.TempDir()
		script := filepath.Join(dir, "bad-sshd")
		err := os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0o755)
		require.NoError(t, err)

		old := _sshdPath
		_sshdPath = script
		t.Cleanup(func() { _sshdPath = old })

		paths := authorizedKeysPaths("charlie")
		assert.Equal(t, []string{".ssh/authorized_keys"}, paths)
	})

	t.Run("falls back when authorizedkeysfile missing from output", func(t *testing.T) {
		dir := t.TempDir()
		script := filepath.Join(dir, "no-keys-sshd")
		err := os.WriteFile(script, []byte("#!/bin/sh\necho 'port 22'\n"), 0o755)
		require.NoError(t, err)

		old := _sshdPath
		_sshdPath = script
		t.Cleanup(func() { _sshdPath = old })

		paths := authorizedKeysPaths("dave")
		assert.Equal(t, []string{".ssh/authorized_keys"}, paths)
	})
}

func TestExpandAuthorizedKeysPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		homeDir  string
		username string
		want     string
	}{
		{
			name:     "relative path joined with home",
			path:     ".ssh/authorized_keys",
			homeDir:  "/home/alice",
			username: "alice",
			want:     "/home/alice/.ssh/authorized_keys",
		},
		{
			name:     "absolute path unchanged",
			path:     "/etc/ssh/authorized_keys.d/alice",
			homeDir:  "/home/alice",
			username: "alice",
			want:     "/etc/ssh/authorized_keys.d/alice",
		},
		{
			name:     "%h expands to homeDir",
			path:     "%h/.ssh/authorized_keys",
			homeDir:  "/home/bob",
			username: "bob",
			want:     "/home/bob/.ssh/authorized_keys",
		},
		{
			name:     "%u expands to username",
			path:     "/etc/ssh/authorized_keys.d/%u",
			homeDir:  "/home/bob",
			username: "bob",
			want:     "/etc/ssh/authorized_keys.d/bob",
		},
		{
			name:     "%% expands to literal percent",
			path:     "/etc/ssh/%%backup",
			homeDir:  "/home/carol",
			username: "carol",
			want:     "/etc/ssh/%backup",
		},
		{
			name:     "%%u is literal percent followed by u",
			path:     "/etc/ssh/%%u",
			homeDir:  "/home/carol",
			username: "carol",
			want:     "/etc/ssh/%u",
		},
		{
			name:     "combined tokens",
			path:     "%h/.ssh/%u_keys",
			homeDir:  "/home/dave",
			username: "dave",
			want:     "/home/dave/.ssh/dave_keys",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := expandAuthorizedKeysPath(tt.path, tt.homeDir, tt.username)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCheckAuthorizedKeysFile(t *testing.T) {
	t.Parallel()

	_, pubkey := testKeyPair(t)
	authorizedLine := ssh.MarshalAuthorizedKey(pubkey)

	t.Run("key found", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "authorized_keys")
		err := os.WriteFile(path, authorizedLine, 0o600)
		require.NoError(t, err)

		assert.True(t, checkAuthorizedKeysFile(path, pubkey))
	})

	t.Run("key not found", func(t *testing.T) {
		t.Parallel()

		_, otherPub := testKeyPair(t)
		otherLine := ssh.MarshalAuthorizedKey(otherPub)

		dir := t.TempDir()
		path := filepath.Join(dir, "authorized_keys")
		err := os.WriteFile(path, otherLine, 0o600)
		require.NoError(t, err)

		assert.False(t, checkAuthorizedKeysFile(path, pubkey))
	})

	t.Run("nonexistent file", func(t *testing.T) {
		t.Parallel()
		assert.False(t, checkAuthorizedKeysFile("/nonexistent/authorized_keys", pubkey))
	})
}

// TestIsKeyAuthorizedMultiplePaths is not parallel because it mutates _sshdPath.
func TestIsKeyAuthorizedMultiplePaths(t *testing.T) {
	_, pubkey := testKeyPair(t)
	authorizedLine := ssh.MarshalAuthorizedKey(pubkey)

	// Create two authorized_keys files. The key is only in the second.
	homeDir := t.TempDir()
	sshDir := filepath.Join(homeDir, ".ssh")
	require.NoError(t, os.MkdirAll(sshDir, 0o700))

	emptyPath := filepath.Join(sshDir, "authorized_keys")
	require.NoError(t, os.WriteFile(emptyPath, []byte(""), 0o600))

	etcDir := filepath.Join(t.TempDir(), "etc", "ssh", "authorized_keys.d")
	require.NoError(t, os.MkdirAll(etcDir, 0o755))
	secondPath := filepath.Join(etcDir, "testuser")
	require.NoError(t, os.WriteFile(secondPath, authorizedLine, 0o644))

	// Create a fake sshd that returns both paths.
	scriptDir := t.TempDir()
	script := filepath.Join(scriptDir, "fake-sshd")
	scriptContent := "#!/bin/sh\necho 'authorizedkeysfile " +
		".ssh/authorized_keys " + secondPath + "'\n"
	require.NoError(t, os.WriteFile(script, []byte(scriptContent), 0o755))

	old := _sshdPath
	_sshdPath = script
	t.Cleanup(func() { _sshdPath = old })

	assert.True(t, isKeyAuthorized(homeDir, "testuser", pubkey),
		"key in secondary path should be found")

	// A different key should not be found.
	_, otherPub := testKeyPair(t)
	assert.False(t, isKeyAuthorized(homeDir, "testuser", otherPub),
		"unknown key should not be found")
}
