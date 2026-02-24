---
status: accepted
date: 2026-02-24
---

# Transport abstraction with Reader/Writer interfaces

## Context and Problem Statement

beam must support multiple backends — local filesystem, SFTP, and the custom beam protocol — without the engine knowing which backend it's using. The engine needs to create endpoints on demand and operate on them uniformly.

## Decision Drivers

* Engine must be decoupled from concrete transport types
* New transports (e.g., S3) should be addable without engine changes
* Transport setup varies: local needs nothing, SFTP needs SSH, beam needs TLS+mux handshake

## Considered Options

* Engine accepts pre-constructed endpoints (original phase 6a approach)
* Engine accepts a `Transport` factory that creates `Reader`/`Writer`/`ReadWriter` on demand

## Decision Outcome

Chosen option: "Transport factory pattern", because it lets the engine call `ReaderAt(path)` or `ReadWriterAt(path)` without knowing connection lifecycle. `LocalTransport` creates fresh local endpoints per call. `beam.Transport` manages a shared mux connection and caches endpoints. `beam.SSHTransport` dials SSH, auto-detects beam daemon, and delegates to `beam.Transport` or falls back to SFTP. Interfaces follow Go stdlib naming: `Reader`, `Writer`, `ReadWriter` (like `io.Reader`/`io.Writer`/`io.ReadWriter`).

### Consequences

* Good, because engine has zero imports from transport-specific packages
* Good, because `beam.SSHTransport` composes auto-detection + fallback transparently
* Bad, because Transport interface adds one level of indirection vs direct endpoint construction
