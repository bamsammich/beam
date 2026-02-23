# beam

**Fast, parallel file copy with delta sync and a beautiful CLI**

[![CI](https://github.com/bamsammich/beam/actions/workflows/ci.yml/badge.svg)](https://github.com/bamsammich/beam/actions/workflows/ci.yml)
[![Go](https://img.shields.io/github/go-mod/go-version/bamsammich/beam)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

beam is a modern file copy tool that replaces `cp` and `rsync` with parallel workers, kernel-level copy offload, delta sync over the network, and beautiful terminal output.

## Features

- **Parallel workers** — copies multiple files simultaneously with auto-tuned worker pools (up to 32 concurrent workers)
- **Kernel fast paths** — uses `copy_file_range(2)` on Linux and `clonefile(2)` on macOS for zero-copy transfers
- **Delta sync** — only transfers changed blocks over network connections using xxHash rolling checksums and BLAKE3 verification
- **Beam protocol** — custom binary protocol with TLS, stream multiplexing, and server-side hashing for fast remote transfers
- **Beautiful CLI** — inline progress with a 2-line HUD, automatic feed/rate view switching, sparkline throughput, and Unicode worker indicators
- **Full-screen TUI** — optional Bubble Tea dashboard (`--tui`) with in-flight file tracking, keybinds, and a rate view with sparkline charts
- **rsync-compatible flags** — familiar `-a`, `-r`, `--delete`, `--exclude`, `--dry-run`, `--partial` so switching is painless
- **Atomic writes** — never leaves partial files at the destination; writes to a temp file and renames on completion

## Installation

### Go install

```bash
go install github.com/bamsammich/beam/cmd/beam@latest
```

### Download a release binary

Download a prebuilt binary from the [releases page](https://github.com/bamsammich/beam/releases) for Linux (amd64, arm64) or macOS (amd64, arm64).

### Build from source

```bash
git clone https://github.com/bamsammich/beam.git
cd beam
make build
# binary is at bin/beam
```

## Quick Start

```bash
# Local recursive copy with archive mode (preserves permissions, timestamps, xattrs)
beam -a ~/photos /backup/photos

# Remote copy via SSH
beam -a /data user@nas:/backup/data

# Remote copy via beam protocol (faster than SFTP)
beam beam://nas:9876/data /local/data

# Dry run — see what would be transferred without writing anything
beam --dry-run -v /src /dst

# Large overnight transfer with full-screen TUI and post-copy verification
beam -a --verify --tui /archive /mnt/nas/archive
```

## Screenshots

> Generate the demo GIFs with [charmbracelet/vhs](https://github.com/charmbracelet/vhs):
>
> ```bash
> vhs assets/demo-inline.tape   # produces assets/demo-inline.gif
> vhs assets/demo-tui.tape      # produces assets/demo-tui.gif
> ```

**Inline mode** — feed view with per-file progress scrolling into a 2-line HUD:

![Inline mode](assets/demo-inline.gif)

**TUI mode** — full-screen dashboard with in-flight files, worker grid, and keybinds:

![TUI mode](assets/demo-tui.gif)

## Benchmarks

### Local transfers (beam vs rsync)

Measured with [hyperfine](https://github.com/sharkdp/hyperfine) (3 runs, 1 warmup) on local ext4 filesystem.

**1 GB file:**

| Command | Mean | Relative |
|:--------|-----:|---------:|
| **`beam`** | **444 ms** | **1.00x** |
| `rsync` | 955 ms | 2.15x slower |

**10,000 x 4 KB files:**

| Command | Mean | Relative |
|:--------|-----:|---------:|
| **`beam -r`** | **203 ms** | **1.00x** |
| `rsync -a` | 257 ms | 1.27x slower |

beam is **2.1x faster than rsync** on large files and **1.27x faster** on many small files for local transfers.

### Network transfers (beam protocol vs rsync over SSH)

Measured with [hyperfine](https://github.com/sharkdp/hyperfine) (3 runs, 1 warmup) over a 1 Gbps Ethernet link.

**1 GB file:**

| Command | Mean | Relative |
|:--------|-----:|---------:|
| **`beam` (beam://)** | **4.81 s** | **1.00x** |
| `rsync` (SSH) | 17.32 s | 3.60x slower |

**10,000 x 4 KB files:**

| Command | Mean | Relative |
|:--------|-----:|---------:|
| **`beam -r` (beam://)** | **1.14 s** | **1.00x** |
| `rsync -r` (SSH) | 1.50 s | 1.32x slower |

beam's custom protocol is **3.6x faster than rsync** for large file transfers thanks to TLS stream multiplexing and zero-overhead framing, and **1.3x faster** for many small files thanks to batch RPCs and a pre-built destination index that eliminates per-file round-trips.

> Run your own benchmarks: `beam --benchmark /src /dst` auto-measures throughput and tunes worker count.

## How It Works

beam's engine pipelines directory scanning and file copying concurrently — copying begins before the full tree is walked. A parallel scanner feeds file tasks through a buffered channel to a pool of copy workers, each writing atomically to a temp file before renaming into place.

- **copy_file_range(2)** / **clonefile(2)** — kernel-level zero-copy for same-device transfers
- **Parallel scanner** — directory tree walking starts copying immediately, no full-tree scan first
- **Atomic writes** — writes to `<dest>.<uuid>.beam-tmp`, then `rename(2)` on success
- **BLAKE3 verification** — optional post-copy checksum pass (`--verify`)
- **Delta transfer** — xxHash rolling checksums + BLAKE3 block verification for network copies
- **Sparse file support** — detects and reproduces hole structure via `SEEK_DATA`/`SEEK_HOLE`
- **Hardlink tracking** — detects `(dev, ino)` pairs and reproduces hardlink graphs
- **Stream multiplexer** — beam protocol multiplexes multiple file transfers over a single TLS connection

## Documentation

For detailed guides, see the [wiki](https://github.com/bamsammich/beam/wiki):

- **[CLI Reference](https://github.com/bamsammich/beam/wiki/CLI-Reference)** — all flags organized by category, daemon and gen-docs subcommands
- **[Remote Transfers](https://github.com/bamsammich/beam/wiki/Remote-Transfers)** — SFTP setup, beam protocol daemon, `beam://` URLs, delta transfer
- **[TUI Guide](https://github.com/bamsammich/beam/wiki/TUI-Guide)** — full-screen mode, keybinds, feed vs rate view, worker adjustment
- **[Filtering and Delete](https://github.com/bamsammich/beam/wiki/Filtering-and-Delete)** — `--exclude`/`--include` patterns, filter files, size filters, `--delete`
- **[Architecture](https://github.com/bamsammich/beam/wiki/Architecture)** — engine pipeline, transport abstraction, platform fast paths, protocol wire format

## License

[MIT](LICENSE)
