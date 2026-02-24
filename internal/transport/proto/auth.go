package proto

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

const nonceSize = 32

// AuthOpts holds client-side authentication parameters.
type AuthOpts struct {
	Username    string     // OS username to authenticate as
	Signer      ssh.Signer // SSH private key signer (from agent or file)
	Fingerprint string     // expected TLS fingerprint (empty = TOFU)
}

// LoadClientAuth builds AuthOpts from the given parameters.
// It discovers an SSH signer from: explicit identity file → agent → default key paths.
func LoadClientAuth(username, identityFile string) (AuthOpts, error) {
	if username == "" {
		u, err := user.Current()
		if err != nil {
			return AuthOpts{}, fmt.Errorf("determine current user: %w", err)
		}
		username = u.Username
	}

	signer, err := findSigner(identityFile)
	if err != nil {
		return AuthOpts{}, fmt.Errorf("no SSH key available: %w", err)
	}

	return AuthOpts{
		Username: username,
		Signer:   signer,
	}, nil
}

// findSigner tries to find an SSH signer in order:
// 1. Explicit identity file
// 2. SSH agent
// 3. Default key paths (~/.ssh/id_ed25519, id_ecdsa, id_rsa)
//
//nolint:revive,ireturn // cognitive-complexity: linear search; ireturn: ssh.Signer is the standard interface
func findSigner(identityFile string) (ssh.Signer, error) {
	// 1. Explicit identity file.
	if identityFile != "" {
		data, readErr := os.ReadFile(identityFile)
		if readErr != nil {
			return nil, fmt.Errorf("read identity file %s: %w", identityFile, readErr)
		}
		signer, parseErr := ssh.ParsePrivateKey(data)
		if parseErr != nil {
			return nil, fmt.Errorf("parse identity file %s: %w", identityFile, parseErr)
		}
		return signer, nil
	}

	// 2. SSH agent.
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		conn, dialErr := net.Dial("unix", sock) //nolint:gosec // SSH_AUTH_SOCK is trusted
		if dialErr == nil {
			agentClient := agent.NewClient(conn)
			signers, signersErr := agentClient.Signers()
			if signersErr == nil && len(signers) > 0 {
				return signers[0], nil
			}
			conn.Close()
		}
	}

	// 3. Default key paths.
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, errors.New("no SSH key found (no agent, no default keys)")
	}
	for _, name := range []string{"id_ed25519", "id_ecdsa", "id_rsa"} {
		keyPath := filepath.Join(home, ".ssh", name)
		data, readErr := os.ReadFile(keyPath)
		if readErr != nil {
			continue
		}
		signer, parseErr := ssh.ParsePrivateKey(data)
		if parseErr != nil {
			continue
		}
		return signer, nil
	}

	return nil, errors.New("no SSH key found (checked agent and default key paths)")
}

// ServerAuth handles the server-side pubkey challenge/response authentication.
type ServerAuth struct {
	// KeyChecker overrides the default authorized_keys lookup. If set, called
	// instead of checking ~/.ssh/authorized_keys. Intended for tests.
	KeyChecker func(username string, pubkey ssh.PublicKey) bool
	root       string // daemon root directory
}

// NewServerAuth creates a server-side authenticator.
func NewServerAuth(root string) *ServerAuth {
	return &ServerAuth{root: root}
}

// Authenticate performs the full pubkey auth flow on a control stream.
// Returns the authenticated username on success.
//
//nolint:gocyclo,revive // cyclomatic,cognitive-complexity: auth flow with multiple validation steps
func (sa *ServerAuth) Authenticate(mux *Mux, controlCh <-chan Frame) (string, error) {
	// Wait for AuthReq with 10s timeout.
	f, ok := timedRecv(controlCh)
	if !ok {
		return "", errors.New("timeout waiting for auth request")
	}
	if f.MsgType != MsgAuthReq {
		return "", fmt.Errorf(
			"expected AuthReq (0x%02x), got 0x%02x", MsgAuthReq, f.MsgType,
		)
	}

	var req AuthReq
	if _, unmarshalErr := req.UnmarshalMsg(f.Payload); unmarshalErr != nil {
		return "", fmt.Errorf("decode AuthReq: %w", unmarshalErr)
	}

	slog.Info("auth request", "username", req.Username, "pubkey_type", req.PubkeyType)

	// Parse the presented public key.
	pubkey, parseErr := ssh.ParsePublicKey(req.Pubkey)
	if parseErr != nil {
		sa.sendAuthResult(mux, false, "invalid public key", "")
		return "", fmt.Errorf("parse pubkey: %w", parseErr)
	}

	// Check authorization.
	if sa.KeyChecker != nil {
		// Custom checker (tests).
		if !sa.KeyChecker(req.Username, pubkey) {
			sa.sendAuthResult(mux, false, "public key not authorized", "")
			return "", fmt.Errorf("key not authorized for %s (custom checker)", req.Username)
		}
	} else {
		// Default: look up OS user and check authorized_keys file.
		u, lookupErr := user.Lookup(req.Username)
		if lookupErr != nil {
			sa.sendAuthResult(mux, false, "unknown user", "")
			return "", fmt.Errorf("user lookup %q: %w", req.Username, lookupErr)
		}
		if !isKeyAuthorized(u.HomeDir, pubkey) {
			sa.sendAuthResult(mux, false, "public key not authorized", "")
			return "", fmt.Errorf("key not in authorized_keys for %s", req.Username)
		}
	}

	// Generate and send challenge.
	nonce := make([]byte, nonceSize)
	if _, randErr := rand.Read(nonce); randErr != nil {
		return "", fmt.Errorf("generate nonce: %w", randErr)
	}

	challenge := AuthChallenge{Nonce: nonce}
	payload, marshalErr := challenge.MarshalMsg(nil)
	if marshalErr != nil {
		return "", marshalErr
	}
	if sendErr := mux.Send(Frame{
		StreamID: ControlStream, MsgType: MsgAuthChallenge, Payload: payload,
	}); sendErr != nil {
		return "", sendErr
	}

	// Wait for AuthResponse.
	f, ok = timedRecv(controlCh)
	if !ok {
		return "", errors.New("timeout waiting for auth response")
	}
	if f.MsgType != MsgAuthResponse {
		return "", fmt.Errorf(
			"expected AuthResponse (0x%02x), got 0x%02x",
			MsgAuthResponse, f.MsgType,
		)
	}

	var resp AuthResponse
	if _, unmarshalErr := resp.UnmarshalMsg(f.Payload); unmarshalErr != nil {
		return "", fmt.Errorf("decode AuthResponse: %w", unmarshalErr)
	}

	// Verify signature.
	sig := new(ssh.Signature)
	if unmarshalErr := ssh.Unmarshal(resp.Signature, sig); unmarshalErr != nil {
		sa.sendAuthResult(mux, false, "invalid signature format", "")
		return "", fmt.Errorf("unmarshal signature: %w", unmarshalErr)
	}

	if verifyErr := pubkey.Verify(nonce, sig); verifyErr != nil {
		sa.sendAuthResult(mux, false, "signature verification failed", "")
		return "", fmt.Errorf("verify signature: %w", verifyErr)
	}

	// Success.
	sa.sendAuthResult(mux, true, "", sa.root)
	slog.Info("authenticated", "username", req.Username)
	return req.Username, nil
}

func (*ServerAuth) sendAuthResult(mux *Mux, ok bool, message, root string) {
	result := AuthResult{OK: ok, Message: message, Root: root}
	payload, marshalErr := result.MarshalMsg(nil)
	if marshalErr != nil {
		return
	}
	_ = mux.Send(Frame{ //nolint:errcheck // best-effort
		StreamID: ControlStream, MsgType: MsgAuthResult, Payload: payload,
	})
}

// keysEqual compares two SSH public keys by their wire-format bytes.
func keysEqual(a, b ssh.PublicKey) bool {
	return bytes.Equal(a.Marshal(), b.Marshal())
}

// isKeyAuthorized checks if pubkey appears in ~user/.ssh/authorized_keys.
func isKeyAuthorized(homeDir string, pubkey ssh.PublicKey) bool {
	authKeysPath := filepath.Join(homeDir, ".ssh", "authorized_keys")
	f, err := os.Open(authKeysPath)
	if err != nil {
		slog.Debug("cannot open authorized_keys", "path", authKeysPath, "error", err)
		return false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		candidate, _, _, _, parseErr := ssh.ParseAuthorizedKey(line)
		if parseErr != nil {
			continue
		}
		if keysEqual(candidate, pubkey) {
			return true
		}
	}
	return false
}

// ClientAuth performs the client side of the pubkey auth flow.
// It sends AuthReq, receives AuthChallenge, signs the nonce, sends
// AuthResponse, and receives AuthResult.
//
//nolint:gocyclo,revive // cyclomatic,cognitive-complexity: auth flow
func ClientAuth(mux *Mux, opts AuthOpts) (string, error) {
	controlCh := mux.OpenStream(ControlStream)
	defer mux.CloseStream(ControlStream)

	pubkey := opts.Signer.PublicKey()

	req := AuthReq{
		Username:   opts.Username,
		Pubkey:     pubkey.Marshal(),
		PubkeyType: pubkey.Type(),
	}
	payload, marshalErr := req.MarshalMsg(nil)
	if marshalErr != nil {
		return "", fmt.Errorf("marshal AuthReq: %w", marshalErr)
	}
	if sendErr := mux.Send(Frame{
		StreamID: ControlStream, MsgType: MsgAuthReq, Payload: payload,
	}); sendErr != nil {
		return "", fmt.Errorf("send AuthReq: %w", sendErr)
	}

	// Wait for AuthChallenge or AuthResult (early rejection).
	f, ok := timedRecv(controlCh)
	if !ok {
		return "", errors.New("timeout waiting for auth challenge")
	}

	// Early rejection: server may send AuthResult{ok:false} directly if
	// user/key lookup failed.
	if f.MsgType == MsgAuthResult {
		var result AuthResult
		if _, unmarshalErr := result.UnmarshalMsg(f.Payload); unmarshalErr != nil {
			return "", fmt.Errorf("decode AuthResult: %w", unmarshalErr)
		}
		if !result.OK {
			return "", fmt.Errorf("authentication failed: %s", result.Message)
		}
		return result.Root, nil
	}

	if f.MsgType != MsgAuthChallenge {
		return "", fmt.Errorf(
			"expected AuthChallenge (0x%02x), got 0x%02x",
			MsgAuthChallenge, f.MsgType,
		)
	}

	var challenge AuthChallenge
	if _, unmarshalErr := challenge.UnmarshalMsg(f.Payload); unmarshalErr != nil {
		return "", fmt.Errorf("decode AuthChallenge: %w", unmarshalErr)
	}

	// Sign the nonce.
	sig, signErr := opts.Signer.Sign(rand.Reader, challenge.Nonce)
	if signErr != nil {
		return "", fmt.Errorf("sign nonce: %w", signErr)
	}

	resp := AuthResponse{Signature: ssh.Marshal(sig)}
	payload, marshalErr = resp.MarshalMsg(nil)
	if marshalErr != nil {
		return "", fmt.Errorf("marshal AuthResponse: %w", marshalErr)
	}
	if sendErr := mux.Send(Frame{
		StreamID: ControlStream, MsgType: MsgAuthResponse, Payload: payload,
	}); sendErr != nil {
		return "", fmt.Errorf("send AuthResponse: %w", sendErr)
	}

	// Wait for AuthResult.
	f, ok = timedRecv(controlCh)
	if !ok {
		return "", errors.New("timeout waiting for auth result")
	}
	if f.MsgType != MsgAuthResult {
		return "", fmt.Errorf(
			"expected AuthResult (0x%02x), got 0x%02x",
			MsgAuthResult, f.MsgType,
		)
	}

	var result AuthResult
	if _, unmarshalErr := result.UnmarshalMsg(f.Payload); unmarshalErr != nil {
		return "", fmt.Errorf("decode AuthResult: %w", unmarshalErr)
	}
	if !result.OK {
		return "", fmt.Errorf("authentication failed: %s", result.Message)
	}

	return result.Root, nil
}

// timedRecv receives a frame from a channel with a 10-second timeout.
func timedRecv(ch <-chan Frame) (Frame, bool) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case f, ok := <-ch:
		if !ok {
			return Frame{}, false
		}
		return f, true
	case <-timer.C:
		return Frame{}, false
	}
}
