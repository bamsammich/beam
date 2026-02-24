---
status: accepted
date: 2026-02-24
---

# Zero-config daemon with auto-token and discovery file

## Context and Problem Statement

The beam daemon needs authentication to prevent unauthorized file access, but requiring manual token configuration creates friction for single-machine or trusted-network setups. The daemon should be runnable with zero configuration while still being secure by default.

## Decision Drivers

* `beam daemon` must work with zero flags for simple setups
* Authentication must be on by default (no unauthenticated mode)
* Other tools (SSH auto-detection) need to discover the daemon's address and token

## Considered Options

* Require manual token configuration via flag or config file
* Auto-generate token on startup, write discovery file to well-known path

## Decision Outcome

Chosen option: "Auto-generate token, write discovery file", because it achieves zero-config operation while keeping auth mandatory. On startup, the daemon generates a 32-byte hex token via `crypto/rand`, binds to port 9876 (default), and writes `{ port, token, root }` to `/etc/beam/daemon.toml` (mode 0644). On shutdown, the discovery file is removed. Default root is `/`, allowing access to the full filesystem (scoped by OS permissions).

### Consequences

* Good, because `beam daemon` works with zero arguments while still requiring bearer token auth
* Good, because SSH auto-detection can read `/etc/beam/daemon.toml` to discover and tunnel to the daemon
* Bad, because discovery file at `/etc/beam/` requires root or appropriate permissions to write
