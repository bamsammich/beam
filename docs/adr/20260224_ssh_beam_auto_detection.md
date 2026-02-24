---
status: accepted
date: 2026-02-24
---

# SSH beam daemon auto-detection with transparent protocol upgrade

## Context and Problem Statement

Users connecting via `user@host:path` syntax expect SSH-based transfers (like rsync). If the remote host runs a beam daemon, beam should automatically upgrade to its faster native protocol without requiring the user to know about the daemon or change their command.

## Decision Drivers

* `user@host:path` must work whether or not a beam daemon is running
* beam protocol should be preferred when available (faster for small files, supports batch RPC)
* Fallback to SFTP must be seamless — no user-visible errors
* Users must be able to force SFTP if needed

## Considered Options

* Require explicit `beam://` URLs for beam protocol, `user@host:path` always uses SFTP
* Auto-detect: read `/etc/beam/daemon.toml` over SFTP, tunnel TLS through SSH if found

## Decision Outcome

Chosen option: "Auto-detect and tunnel through SSH", because it gives users beam protocol performance without any configuration. `beam.SSHTransport` opens an SSH connection, reads `/etc/beam/daemon.toml` via SFTP, and if a daemon is found, tunnels a TLS connection to `localhost:<port>` through the SSH channel, completing the beam handshake transparently. If no daemon or discovery file is found, it falls back to plain SFTP. The `--no-beam-ssh` flag forces SFTP fallback.

### Consequences

* Good, because users get beam protocol benefits (batch RPC, server-side delta) transparently
* Good, because SSH provides user authentication — the daemon token is auto-discovered and supplied, so the user needs no beam-specific credentials
* Bad, because tunneling TLS through SSH adds overhead vs direct `beam://` connections (~20% for large files)
