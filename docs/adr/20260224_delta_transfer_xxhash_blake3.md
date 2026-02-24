---
status: accepted
date: 2026-02-24
---

# Delta transfer with xxHash rolling hash and BLAKE3 strong hash

## Context and Problem Statement

When copying files that already partially exist at the destination (e.g., log rotation, VM images, database dumps), transferring only the changed blocks saves significant bandwidth. beam needs an rsync-style delta algorithm with modern hash choices.

## Decision Drivers

* Must minimize bytes transferred for partially-changed large files
* Rolling hash must be fast enough to not bottleneck on local disk I/O
* Strong hash must be collision-resistant for block matching correctness

## Considered Options

* rsync's adler32 rolling + MD5 strong
* xxHash rolling + BLAKE3 strong
* Full-file BLAKE3 comparison only (no delta, just skip or copy)

## Decision Outcome

Chosen option: "xxHash rolling + BLAKE3 strong", because xxHash is ~3x faster than adler32 and BLAKE3 is ~10x faster than MD5 while being cryptographically stronger. Block size is `sqrt(filesize)` clamped to [512B, 128KB], balancing match granularity against signature overhead. Delta is on by default for network transports. Server-side RPCs (`ComputeSignature`, `MatchBlocks`, `ApplyDelta`) keep hash computation local to the data.

### Consequences

* Good, because delta transfer reduces bandwidth by 90%+ for partially-changed files
* Good, because server-side hashing avoids transferring full file content just to compute signatures
* Bad, because delta adds latency for small files where full transfer would be faster (mitigated by the batcher handling small files separately)
