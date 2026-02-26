---
status: accepted
date: 2026-02-25
---

# Per-connection zstd stream compression negotiated via pre-mux byte exchange

## Context and Problem Statement

All beam protocol data is transferred uncompressed. TLS 1.3 removed compression, and the custom mux sends raw msgpack-framed bytes. On bandwidth-constrained links, compressible files (source code, logs, configs) transfer slower than necessary.

## Decision Drivers

* Must not break backward compatibility — old clients that omit the new field must connect without compression
* Must not regress LAN throughput — incompressible data should pay near-zero CPU cost
* Must integrate with the existing mux flush strategy without introducing latency stalls
* Compression flag is a transport-construction option, not a runtime capability interface query

## Considered Options

* Per-connection compression (wrap conn under mux with zstd streaming encoder/decoder)
* Per-stream compression (wrap beamWriteFile/beamReader per file)
* Per-frame compression (compress individual frame payloads, add flag to frame header)

## Decision Outcome

Chosen option: "Per-connection compression", because it has a single insertion point, gives the best compression ratios (cross-file dictionary), and automatically covers batch writes and all message types without changes to the mux frame format or message types.

Compression is negotiated via a 1-byte exchange after TLS handshake, before the mux starts. The client sends `0x01` (request compression) or `0x00` (no compression); the server echoes its agreement. When both agree, both sides wrap the TLS conn with a `compressedConn` (zstd encoder/decoder) and pass it to `NewMux()`. The mux is entirely unaware of compression. `HandshakeReq`/`HandshakeResp` are dead code and not used for this negotiation.

When compression is active, the `bufio.Writer` in the mux write loop is removed from the stack. The `compressedConn` implements a `WriteFlusher` interface; the mux detects this at construction and uses it for drain-triggered flushing instead of `bufio.Writer.Flush()`. The zstd encoder's internal buffer replaces the `bufio.Writer`, avoiding two-level buffering and preserving the mux ADR's latency-sensitive flush semantics.

Algorithm: zstd level 1 (`SpeedFastest`) via `github.com/klauspost/compress/zstd`. Built-in incompressibility detector short-circuits on already-compressed data with ~0.5% expansion.

### Consequences

* Good, because compressible files transfer 2-10x fewer bytes on bandwidth-constrained links
* Good, because zstd's incompressibility detector makes the CPU cost near-zero for already-compressed data
* Bad, because zstd encoder/decoder allocate ~1-2 MB per connection; high connection counts on the daemon increase memory pressure
* Bad, because `github.com/klauspost/compress` becomes a direct dependency (already indirect via other deps)
