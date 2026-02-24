---
status: accepted
date: 2026-02-24
---

# Stream multiplexer over single TLS connection

## Context and Problem Statement

The beam protocol needs to support concurrent file transfers, RPC calls, and streaming walks over a network connection. Opening a new TCP+TLS connection per operation would add significant latency and resource overhead, especially for small-file workloads.

## Decision Drivers

* Must support concurrent streams (file data + RPCs) without head-of-line blocking between streams
* Connection setup cost (TCP handshake + TLS) should be paid once, not per-file
* Must achieve high throughput on bulk transfers despite multiplexing overhead

## Considered Options

* One TCP+TLS connection per concurrent operation
* HTTP/2 multiplexing
* Custom stream multiplexer over a single TLS connection

## Decision Outcome

Chosen option: "Custom stream multiplexer", because it avoids HTTP/2's header overhead and gives full control over frame sizing and flush behavior. The mux uses a frame format (header + payload) with `TCP_NODELAY` on all connections. The write loop uses a 64KB `bufio.Writer` that flushes only when the write channel is drained, batching small frames into fewer syscalls. `WriteFrame` combines header and payload into a single `Write()` call to avoid Nagle-induced delays.

### Consequences

* Good, because single connection pays TLS handshake cost once; all streams share it
* Good, because smart flush strategy reduces syscall count without adding latency
* Bad, because custom mux is more code to maintain than using an existing library (e.g., yamux, smux)
