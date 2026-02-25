---
status: accepted
date: 2026-02-24
supersedes: 20260224_zero_config_daemon.md
---

# TLS fingerprint pinning + SSH pubkey auth + fork-per-connection

## Context and Problem Statement

The beam daemon uses a shared bearer token for authentication with `InsecureSkipVerify: true` on the client TLS config. This means no user identity (anyone with the token gets the daemon process's full permissions), no MITM protection for direct `beam://` connections, and no filesystem permission scoping per user.

## Decision Drivers

* Filesystem operations must run with the authenticated user's OS permissions, not the daemon's
* Direct `beam://` connections must be protected against MITM (currently self-signed + skip verify)
* Must reuse existing SSH key infrastructure — no new credential systems to manage
* Fork-per-connection cost (~5ms, ~15-20MB) is negligible for long-lived beam transfers

## Considered Options

* Keep bearer token, add per-user token files with setuid helper
* SSH pubkey challenge/response inside TLS, fork child process as authenticated user
* Embed a full SSH server in the daemon

## Decision Outcome

Chosen option: "SSH pubkey challenge/response + fork-per-connection", because it leverages existing `~/.ssh/authorized_keys` infrastructure, provides real OS-enforced permission isolation, and avoids the complexity of embedding a full SSH server.

**Authentication flow:** Client connects over TLS (fingerprint-verified) → sends `AuthReq{username, pubkey}` → server looks up `~user/.ssh/authorized_keys`, sends `AuthChallenge{nonce}` → client signs nonce → server verifies → sends `AuthResult` → server drops privileges to authenticated user via `setuid`/`setgid` → runs beam mux/handler.

**TLS termination:** In fork mode, the parent accepts raw TCP and forks immediately. The child process loads the TLS certificate from disk, performs the TLS handshake, and owns the full TLS session. This is necessary because Go's `crypto/tls` stores session state in process memory — a child process cannot resume a parent's TLS session. Security properties are preserved: the client still verifies the TLS fingerprint and performs SSH pubkey auth, just with the child process.

**Privilege model:** In fork mode, the child starts as root (inherited from the parent daemon). After TLS handshake and successful authentication, it calls `syscall.Setgroups` → `syscall.Setgid` → `syscall.Setuid` to drop to the authenticated user. The root window is brief (TLS handshake + auth exchange, typically <100ms) and mirrors OpenSSH's privilege separation pattern. This differs from the original design of using `SysProcAttr.Credential` at exec time, which is not possible because authentication must happen inside the child (to have TLS context), so the UID is not known at fork time.

**TLS verification:** Daemon generates a persistent self-signed cert on first run (stored at `/etc/beam/daemon.crt`, `/etc/beam/daemon.key`). Discovery file contains `fingerprint` instead of `token`. SSH-discovered connections verify fingerprint from the trusted SSH channel. Direct `beam://` uses TOFU with `~/.config/beam/known_hosts`. `--fingerprint` flag allows explicit pinning.

**Connection paths:**
- Direct `beam://`: TCP → fork → TLS (TOFU verify) → pubkey auth → privilege drop → mux
- `user@host:path` (auto): SSH → read discovery → TCP → fork → TLS (fingerprint verify) → pubkey auth → privilege drop → mux
- `user@host:path --beam-tunnel`: SSH → read discovery → TCP-over-SSH-tunnel → fork → TLS (fingerprint verify) → pubkey auth → privilege drop → mux
- `user@host:path` (fallback): SFTP over SSH
- In-process mode (`--no-fork`): TLS (in parent) → pubkey auth → mux (no fork, no privilege drop)

### Consequences

* Good, because filesystem operations run as the authenticated user — kernel enforces permissions naturally
* Good, because reuses SSH key infrastructure (`authorized_keys`, `ssh-agent`) — no new credentials to manage
* Good, because TLS fingerprint pinning (TOFU or discovery-pinned) eliminates MITM for direct connections
* Good, because child owns full TLS lifecycle — no cross-process TLS state transfer needed
* Bad, because daemon must run as root (or `CAP_SETUID`/`CAP_SETGID`) to fork as other users
* Bad, because brief root window in child (TLS handshake + auth) before privilege drop
* Bad, because breaking protocol change — old clients cannot authenticate with new daemons

## Pros and Cons of the Options

### Per-user token files with setuid helper

* Good, because no protocol changes needed
* Bad, because requires managing per-user token files and a setuid binary — complex and fragile

### Full embedded SSH server

* Good, because complete SSH compatibility
* Bad, because massive implementation complexity and attack surface for features beam doesn't need
