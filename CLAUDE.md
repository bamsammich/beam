# beam — Development Guide

## Overview

`beam` is a modern file copy CLI tool written in Go. It is designed to be:

1. **Fast** — faster than `rsync` for local copies via parallel workers, kernel copy offload, and io_uring; competitive on network transfers via a custom binary protocol with batch small-file RPC
2. **Reliable** — atomic writes, optional post-copy checksum verification, mtime-based skip detection, and correct handling of edge cases (sparse files, hardlinks, xattrs)
3. **Beautiful** — inline terminal output with an optional full TUI (`--tui`) for large/long-running transfers

Module path: `github.com/bamsammich/beam`

---

## Technology Stack

| Concern | Choice |
|---|---|
| Language | Go |
| TUI framework | Bubble Tea (`github.com/charmbracelet/bubbletea`) |
| TUI styling | Lip Gloss (`github.com/charmbracelet/lipgloss`) |
| CLI parsing | `github.com/spf13/cobra` |
| Checksums | BLAKE3 (`github.com/zeebo/blake3`) |
| Rolling hash (delta) | xxHash (`github.com/cespare/xxhash/v2`) |
| SSH transport | `golang.org/x/crypto/ssh` + `github.com/pkg/sftp` |
| Wire format | msgpack via `tinylib/msgp` (code-gen, map encoding) |
| Testing | stdlib `testing` + `testify` |

---

## Architecture

### Component Map

```
cmd/beam/
  main.go                  — entrypoint, cobra root command

internal/
  engine/
    engine.go              — orchestrator, wires endpoints into components
    scanner.go             — parallel directory walker, emits FileTask
    worker.go              — copy worker pool, consumes FileTask
    batcher.go             — small-file batch accumulator for beam protocol
    verify.go              — post-copy BLAKE3 verification pass
    delete.go              — extraneous file deletion pass
    sparse.go              — sparse file detection via SEEK_DATA/SEEK_HOLE

  platform/
    copy_linux.go          — copy_file_range(2), io_uring (build tag: linux)
    copy_darwin.go         — clonefile(2) for APFS (build tag: darwin)
    copy_fallback.go       — read/write with large aligned buffers

  transport/
    transport.go           — ReadEndpoint / WriteEndpoint interfaces, FileEntry, Capabilities
    local.go               — local filesystem endpoints
    sftp.go                — SSH/SFTP endpoints
    ssh.go                 — SSH connection management
    delta.go               — rsync-style block matching delta transfer
    location.go            — user@host:path location parsing
    beam/
      endpoint.go          — beam protocol ReadEndpoint + WriteEndpoint (incl. WriteFileBatch)
    proto/
      daemon.go            — standalone TCP daemon (beam daemon)
      mux.go               — stream multiplexer over single TLS connection
      frame.go             — wire frame format (header + payload)
      messages.go          — msgpack message types (go:generate msgp)
      messages_gen.go      — generated marshaling code
      handler.go           — server-side RPC dispatch
      convert.go           — wire ↔ transport type conversions

  ui/
    inline.go              — inline progress: ANSI HUD, feed mode, rate mode
    tui/
      model.go             — Bubble Tea root model
      feed.go              — feed view component
      rate.go              — rate view component (sparkline, worker grid)
      theme.go             — Catppuccin Mocha color palette

  stats/
    collector.go           — lock-free stats aggregation (atomic ops)
    sparkline.go           — rolling throughput ring buffer

  filter/
    rules.go               — include/exclude glob pattern engine
```

### Data Flow

```
[Scanner goroutine pool]
        │  chan FileTask (buffered, size = 2 × workers)
        ▼
[Batcher]  (beam:// only — coalesces small files into batch RPCs)
        │
        ▼
[Worker goroutine pool]  ──→  [Stats Collector]  ──→  [UI renderer]
        │
        ▼  (if --verify)
[Verify goroutine pool]
```

### Protocol Backward Compatibility (STRICT)

All beam protocol messages use msgpack map encoding (field names as string keys, NOT positional). New fields MUST be additive with zero-value defaults. Old clients MUST ignore unknown fields. Never remove or rename a field. This rule applies to all types in `internal/transport/proto/messages.go`.

### Batch Small-File RPC

When transferring to a `beam://` destination, small regular files (< 64KB) are accumulated by the batcher into batches of up to 100 files / 4MB total. Each batch is sent as a single `WriteFileBatchReq` RPC (message type 0x33), processed atomically per-file on the server, and returns per-file results in `WriteFileBatchResp` (0x34). This reduces per-file round-trips from 5 to 1/N.

### Path Translation & Destination Index

Beam endpoints translate between client-relative and server-relative paths using a `pathPrefix` computed from the daemon root (obtained during handshake) and the user-specified path. Before scanning, the engine pre-walks the destination via a streaming `Walk` RPC (with subtree support via `WalkReq.RelPath`) and builds a `DstIndex` map for O(1) skip detection, eliminating per-file `Stat` RPCs over the network.

### TCP/Mux Optimizations

- `TCP_NODELAY` is set on all beam connections
- The mux write loop uses a 64KB `bufio.Writer` that flushes only when the write channel is drained
- `WriteFrame` combines header + payload into a single `Write()` call

---

## Key Design Rules

- **Atomic writes**: Never write directly to the destination path. Always write to `<dest>.<uuid>.beam-tmp`, then `os.Rename`. Crash/ctrl-c never leaves a partial file.
- **Delta transfer**: Uses xxHash rolling checksums + BLAKE3 strong hash. Block size: `sqrt(filesize)` clamped to [512B, 128KB]. On by default for network transports.
- **Skip detection**: mtime + size match = skip (same as rsync default).
- **Batch RPC is beam-specific**: Not on the `WriteEndpoint` interface. Accessed via type assertion, same pattern as delta transfer methods.
- **One failure doesn't abort a batch**: Server processes each entry independently, returns per-file OK/Error.

---

## Building and Testing

```bash
make build     # go build -ldflags="-s -w" -o bin/beam ./cmd/beam
make test      # go test -race ./...
make bench     # go test -bench=. -benchmem ./...
make lint      # staticcheck ./... && golangci-lint run
```

After modifying `internal/transport/proto/messages.go`, regenerate msgp code:
```bash
go generate ./internal/transport/proto/
```

---

## Current Status

All initial phases are implemented:

- **Phase 1** — Core engine with local copy
- **Phase 2+3** — Event system, inline UI, filtering, and delete
- **Phase 4** — BLAKE3 post-copy verification, mtime-based skip detection
- **Phase 5** — TUI, config system, worker throttle, multi-source support
- **Phase 6a** — Transport abstraction, SFTP endpoints, SSH auth, push-only delta transfer
- **Phase 6b** — Custom binary protocol: daemon, `beam://` URL scheme, msgpack wire format, TLS+bearer auth, stream multiplexer, server-side hashing
- **Phase 6c** — Server-side delta transfer: ComputeSignature/MatchBlocks/ApplyDelta RPCs, push/pull/beam-to-beam delta
- **Phase 7** — Polish: `--bwlimit`, `--benchmark`, JSON logging, man pages, CI/CD
- **Batch RPC** — WriteFileBatchReq/Resp for small-file batching, TCP/mux optimizations
- **Path fix + DstIndex** — Beam endpoint path translation, subtree Walk, pre-built destination index for skip detection

### Next Up
- **Phase 6d** — SSH beam auto-detection
  - Auto-detect beam on remote: try `beam --server` over SSH, fall back to SFTP

### Known Issues

- **`--bwlimit` has no effect on local copies.** The kernel fast paths (`copy_file_range`, `sendfile`) bypass userspace entirely, so the `rate.Limiter` wrapping `io.Reader`/`io.Writer` is never hit. Fix: when `--bwlimit` is set, skip the kernel fast path and fall back to the userspace read/write loop where the limiter lives.
- **VHS demo GIFs need re-recording.** The current inline and TUI demo GIFs complete too fast to show the HUD. Re-record after bwlimit is fixed for local copies, or use a real network transfer between two machines.

---

## Non-Goals (v1)

- GUI
- Windows support (Linux and macOS only for v1)
- S3 / object storage transport (transport interface supports future extension)
- Encryption at rest (transport uses TLS)

---

## Wiki Maintenance

The GitHub wiki lives at `github.com/bamsammich/beam.wiki`. When changes to beam affect CLI flags, transport behavior, TUI keybinds, filtering, architecture, or any other documented behavior, **update the corresponding wiki page(s)** to stay in sync:

| Wiki Page | Update when... |
|-----------|----------------|
| CLI-Reference | Flags are added, removed, or have default changes |
| Remote-Transfers | SFTP, beam protocol, delta transfer, or daemon behavior changes |
| TUI-Guide | TUI keybinds, views, or theme changes |
| Filtering-and-Delete | Filter syntax, size filter, or `--delete` behavior changes |
| Architecture | Engine pipeline, transport interfaces, protocol wire format, or stats collector changes |
| Home | Navigation links need updating (new pages added) |
