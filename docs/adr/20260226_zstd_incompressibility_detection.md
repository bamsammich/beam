---
status: accepted
date: 2026-02-26
---

# Rely on zstd level 1 incompressibility detection over application-level content-aware compression

## Context and Problem Statement

Beam's per-connection zstd compression processes all data including already-compressed files (video, images, archives). Should we implement application-level content-aware suppression (per-frame compression with extension-based skip), or rely on zstd's built-in incompressibility detection at level 1?

## Decision Drivers

* Prefer CPU cost over network cost — always compress to maximize wire savings
* Minimize architectural complexity — avoid protocol changes when the compressor already handles the case
* zstd level 1 incompressibility detection runs at 5-10 GB/s, well above typical network throughput

## Considered Options

* Per-frame compression with extension-based skip (compress individual payloads, skip known-compressed extensions)
* Rely on zstd level 1 built-in incompressibility detection (current behavior, no code change)

## Decision Outcome

Chosen option: "Rely on zstd level 1 built-in incompressibility detection", because the match finder at level 1 already short-circuits on incompressible data at memory-bandwidth speed (~5-10 GB/s), producing ~0.5% expansion with near-zero CPU cost relative to network throughput. Per-frame compression would add protocol complexity (frame header change, negotiation mode, per-payload codec) for negligible practical benefit and would lose cross-frame dictionary compression ratios on compressible data.

### Consequences

* Good, because no protocol or architectural changes are needed
* Good, because cross-frame dictionary benefits are preserved for compressible data (better compression ratios)
* Bad, because higher zstd levels (3+) would waste more CPU on incompressible data — revisit if the compression level is raised
