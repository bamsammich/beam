package transport

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// SSHOpts configures SSH connection behavior.
type SSHOpts struct {
	Port     int    // 0 = default (22)
	KeyFile  string // override key file path; empty = try defaults
	Password string // for non-interactive; empty = skip password auth
}

// DialSSH establishes an SSH connection to host as user.
//
// Auth methods are tried in order:
//  1. SSH agent (if SSH_AUTH_SOCK is set)
//  2. Key files (~/.ssh/id_ed25519, id_ecdsa, id_rsa) or SSHOpts.KeyFile
//  3. Password (if SSHOpts.Password is set)
func DialSSH(host, userName string, opts SSHOpts) (*ssh.Client, error) {
	if userName == "" {
		u, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("determine current user: %w", err)
		}
		userName = u.Username
	}

	port := opts.Port
	if port == 0 {
		port = 22
	}

	authMethods := buildAuthMethods(opts)
	if len(authMethods) == 0 {
		return nil, fmt.Errorf("no SSH auth methods available (set SSH_AUTH_SOCK, provide a key, or password)")
	}

	hostKeyCallback, err := defaultHostKeyCallback()
	if err != nil {
		// Fall back to insecure if known_hosts can't be loaded.
		// This matches the behavior of most CLI tools on first connection.
		//nolint:gosec // fallback for systems without known_hosts
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	}

	config := &ssh.ClientConfig{
		User:            userName,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}

	return client, nil
}

func buildAuthMethods(opts SSHOpts) []ssh.AuthMethod {
	var methods []ssh.AuthMethod

	// 1. SSH agent.
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		conn, err := net.Dial("unix", sock)
		if err == nil {
			agentClient := agent.NewClient(conn)
			methods = append(methods, ssh.PublicKeysCallback(agentClient.Signers))
		}
	}

	// 2. Key files.
	if opts.KeyFile != "" {
		if m := keyFileAuth(opts.KeyFile); m != nil {
			methods = append(methods, m)
		}
	} else {
		for _, name := range []string{"id_ed25519", "id_ecdsa", "id_rsa"} {
			home, err := os.UserHomeDir()
			if err != nil {
				continue
			}
			keyPath := filepath.Join(home, ".ssh", name)
			if m := keyFileAuth(keyPath); m != nil {
				methods = append(methods, m)
			}
		}
	}

	// 3. Password.
	if opts.Password != "" {
		methods = append(methods, ssh.Password(opts.Password))
	}

	return methods
}

func keyFileAuth(path string) ssh.AuthMethod {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	signer, err := ssh.ParsePrivateKey(data)
	if err != nil {
		return nil
	}
	return ssh.PublicKeys(signer)
}

func defaultHostKeyCallback() (ssh.HostKeyCallback, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	knownHostsPath := filepath.Join(home, ".ssh", "known_hosts")
	return knownhosts.New(knownHostsPath)
}
