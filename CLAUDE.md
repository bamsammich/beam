# beam — Development Guide

## Overview

`beam` is a modern file copy CLI tool written in Go. It is designed to be:

1. **Fast** — faster than `rsync` for local copies via parallel workers, kernel copy offload, and io_uring; competitive on network transfers via a custom binary protocol with batch small-file RPC
2. **Reliable** — atomic writes, optional post-copy checksum verification, mtime-based skip detection, and correct handling of edge cases (sparse files, hardlinks, xattrs)
3. **Beautiful** — inline terminal output with an optional full TUI (`--tui`) for large/long-running transfers

Module path: `github.com/bamsammich/beam`

## IMPORTANT

- ALWAYS load go-project skill

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

### Known Issues

- **VHS demo GIFs need re-recording.** The current inline and TUI demo GIFs complete too fast to show the HUD. Re-record after bwlimit is fixed for local copies, or use a real network transfer between two machines.
- **Compression is not content-aware.** zstd stream compression is always applied (unless `--no-compress`) even for already-compressed data (video, images, archives). This wastes CPU for zero wire savings. Fix: add content-aware suppression — sample the first chunk of each stream or detect incompressible file extensions, and skip compression for data that won't benefit.

## Non-Goals (v1)

- GUI
- Windows support (Linux and macOS only for v1)
- S3 / object storage transport (transport interface supports future extension)
- Encryption at rest (transport uses TLS)

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
