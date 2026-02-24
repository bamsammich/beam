---
status: accepted
date: 2026-02-24
---

# Capability interfaces for optional transport features

## Context and Problem Statement

Not all transports support the same features. Batch writes are beam-only. Delta transfer requires server-side hashing. Subtree walking is only available on beam endpoints. The engine needs to discover and use these features without type-asserting concrete transport types.

## Decision Drivers

* Engine must not import or reference concrete transport types
* Optional features should be discoverable at runtime, not compile-time flags
* Pattern should be idiomatic Go (like `io.WriterTo`, `io.ReaderFrom`)

## Considered Options

* Capabilities bitfield on the Transport interface
* Go optional interface pattern: define narrow interfaces, type-assert endpoints at point of use

## Decision Outcome

Chosen option: "Go optional interface pattern", because it requires no capability negotiation protocol and is idiomatic Go. Five capability interfaces: `BatchWriter` (batched small-file writes), `DeltaSource` (server-side signature/block matching), `DeltaTarget` (server-side delta application), `SubtreeWalker` (walk relative to endpoint root), `PathResolver` (resolve relative to absolute paths). The engine type-asserts only these interfaces, never concrete types.

### Consequences

* Good, because adding a new capability requires only a new interface + implementation — no engine changes
* Good, because capabilities compose: a beam endpoint can implement all five, SFTP only DeltaSource/DeltaTarget
* Bad, because type assertions fail silently (return false) — missing capability is invisible unless explicitly checked
