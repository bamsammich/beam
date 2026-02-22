# beam — Claude Code Planning Prompt

## Overview

You are helping plan and implement `beam`, a modern file copy CLI tool written in Go. `beam` is designed to be:

1. **Fast** — faster than `rsync` for local copies via parallel workers, kernel copy offload, and io_uring; competitive on network transfers via a custom binary protocol
2. **Reliable** — atomic writes, optional post-copy checksum verification, mtime-based skip detection, and correct handling of edge cases (sparse files, hardlinks, xattrs)
3. **Beautiful** — inline terminal output that feels at home alongside `curl`, `git clone`, and `cargo build`, with an optional full TUI (`--tui`) for large/long-running transfers

The name is `beam`. The binary is `beam`. The GitHub org and module path should be `github.com/nicholasgasior/beam` or similar — settle on a path early and use it consistently throughout.

---

## Design Constraints

- **Default output is inline, not full-screen.** The terminal is not captured. Progress is printed as files complete (scrolling naturally into history) with a 2-line HUD at the bottom that rewrites in place using ANSI cursor control (`\033[nA` + `\033[J`). When stdout is not a TTY, all ANSI is suppressed and output is clean newline-delimited text suitable for piping and logging.
- **`--tui` flag enables a full-screen Bubble Tea TUI** for large transfers where you want a dashboard. This is an opt-in, not the default.
- **Minimal, optional config file.** An optional TOML config at `$XDG_CONFIG_HOME/beam/config.toml` supports theme customization and persistent flag defaults. CLI flags always override config values. beam works with zero configuration — the config file is never required.
- **rsync-compatible flag aliases** where it makes sense (`-r`, `-a`, `--delete`, `--exclude`, `--dry-run`, `--partial`) so rsync users feel at home. Layer beam-specific flags on top.

---

## Technology Stack

| Concern | Choice | Rationale |
|---|---|---|
| Language | Go | Excellent concurrency primitives, single static binary, fast iteration, good syscall access |
| TUI framework | Bubble Tea (`github.com/charmbracelet/bubbletea`) | Functional update model, composable, handles resize correctly |
| TUI styling | Lip Gloss (`github.com/charmbracelet/lipgloss`) | Pairs with Bubble Tea, expressive styling primitives |
| CLI parsing | `github.com/spf13/cobra` | Familiar, composable, good flag UX |
| Checksums | BLAKE3 (`github.com/zeebo/blake3`) | Faster than MD5/SHA, parallelizable, correct |
| Rolling hash (delta) | xxHash (`github.com/cespare/xxhash/v2`) | Faster than Adler-32 used by rsync |
| SSH transport | `golang.org/x/crypto/ssh` + `github.com/pkg/sftp` | Standard, well-maintained |
| Wire format | msgpack via `tinylib/msgp` (code-gen) | Zero-alloc, no reflection, backward-compatible map encoding |
| Testing | stdlib `testing` + `testify` | Standard Go idioms |

---

## Architecture

### High-Level Component Map

```
cmd/beam/
  main.go                  — entrypoint, cobra root command

internal/
  engine/
    engine.go              — orchestrator, wires endpoints into components
    scanner.go             — parallel directory walker, emits FileTask
    worker.go              — copy worker pool, consumes FileTask
    verify.go              — post-copy BLAKE3 verification pass
    delete.go              — extraneous file deletion pass
    sparse.go              — sparse file detection via SEEK_DATA/SEEK_HOLE

  platform/
    copy_linux.go          — copy_file_range(2), io_uring (build tag: linux)
    copy_darwin.go         — clonefile(2) for APFS (build tag: darwin)
    copy_fallback.go       — read/write with large aligned buffers

  transport/
    transport.go           — ReadEndpoint / WriteEndpoint interfaces, FileEntry
    local.go               — local filesystem endpoints (LocalReadEndpoint, LocalWriteEndpoint)
    sftp.go                — SSH/SFTP endpoints (SFTPReadEndpoint, SFTPWriteEndpoint)
    ssh.go                 — SSH connection management (DialSSH, auth chain)
    delta.go               — rsync-style block matching delta transfer
    location.go            — user@host:path location parsing
    protocol.go            — custom binary protocol (future: when both ends are beam)

  ui/
    inline.go              — inline progress: ANSI HUD, feed mode, rate mode
    tui/
      model.go             — Bubble Tea root model
      feed.go              — feed view component
      rate.go              — rate view component (sparkline, worker grid)
      theme.go             — shared Lip Gloss styles and color palette

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
[Worker goroutine pool]  ──→  [Stats Collector]  ──→  [UI renderer]
        │
        ▼  (if --verify)
[Verify goroutine pool]
```

The scanner emits tasks as soon as files are discovered — copying begins before the full directory tree is walked. This is a meaningful improvement over rsync's behavior on large trees.

### Protocol Backward Compatibility (STRICT)

All beam protocol messages use msgpack map encoding (field names as string keys, NOT positional). New fields MUST be additive with zero-value defaults. Old clients MUST ignore unknown fields. Never remove or rename a field. This rule applies to all types in `internal/transport/proto/messages.go`.

---

## Core Engine Specification

### Worker Pool

- Default worker count: `min(runtime.NumCPU() * 2, 32)`, tunable via `--workers N` or `+`/`-` in TUI
- Files above a size threshold (default 64 MB, flag `--chunk-threshold`) are split into chunks and transferred by multiple workers concurrently
- Small files (below threshold) are batched to amortize syscall overhead — a single worker processes a batch of N small files sequentially before returning to the pool

### Platform Copy Fast Paths (in priority order)

**Linux:**
1. `copy_file_range(2)` — same-filesystem copies handled entirely in kernel, zero userspace data movement. Check for `EXDEV` and fall through.
2. `sendfile(2)` — cross-device, still avoids double-copy through userspace
3. `io_uring` — batch async I/O for high-IOPS small-file workloads. Use `github.com/iceber/iouring-go` or implement directly via syscall. Gate behind build tag and runtime kernel version check (requires 5.1+).
4. Fallback: `read`/`write` loop with 1 MB aligned buffer (`unix.Mmap` or `make([]byte, 1<<20)`)

**macOS:**
1. `clonefile(2)` — APFS copy-on-write, instantaneous and free for local same-volume copies. Fall through on non-APFS (`ENOTSUP`).
2. Fallback: `read`/`write` with 1 MB buffer

**All platforms:**
- Detect sparse files via `SEEK_DATA`/`SEEK_HOLE` before copying; reproduce hole structure on destination
- Detect hardlinks by tracking `(dev, ino)` pairs; reproduce hardlink graph rather than duplicating data
- Preserve: mtime, atime (optional), permissions, xattrs, ACLs

### Atomic Writes

Never write directly to the destination path. Always:
1. Write to `<dest>.<uuid>.beam-tmp` in the same directory (same device, guarantees `rename` is atomic)
2. On completion: `os.Rename(tmp, dest)`
3. On failure or interrupt: unlink the tmp file

This means a crash or `ctrl-c` never leaves a partial file at the destination path.

### Delta Transfer (network mode)

When source and destination are remote and `--delta` is set (on by default for network transports):
1. Receiver computes rolling xxHash checksums over fixed blocks of the existing destination file, sends to sender
2. Sender matches blocks, transmits only changed regions
3. Block size is chosen dynamically: `sqrt(filesize)` clamped to [512B, 128KB], not rsync's fixed 700B
4. Strong hash for block verification: BLAKE3 (not MD4 like rsync)

### Skip Detection

beam uses mtime-based skip detection: if a destination file exists with the same size and modification time as the source, the file is skipped. This is the same heuristic rsync uses by default and avoids the overhead of a persistent checkpoint database.

---

## CLI Interface

```
beam [flags] <source> <destination>

Flags:
  # behavior
  -r, --recursive          recurse into directories (default: true for dirs)
  -a, --archive            preserve permissions, timestamps, xattrs, hardlinks
      --delete             delete destination files not present in source
      --dry-run            show what would be transferred without doing it
      --delta              use delta transfer for network copies (default: on)
      --verify             checksum verify after copy (default: off, recommended for NAS)
      --partial            keep partial files on interrupt for resume
  -n, --workers INT        parallel worker count (default: auto)
      --chunk-threshold    file size at which to use multi-worker chunking (default: 64MB)
      --bwlimit RATE       bandwidth limit (e.g. 100MB, 1GB)

  # filtering
      --exclude PATTERN    exclude files matching glob pattern (repeatable)
      --include PATTERN    include override for excluded pattern (repeatable)
      --filter FILE        read filter rules from file (rsync filter syntax)
      --min-size SIZE      skip files smaller than SIZE
      --max-size SIZE      skip files larger than SIZE

  # output
      --tui                full-screen TUI (Bubble Tea) — best for large long-running copies
      --feed               force per-file feed output even above rate-view threshold
      --rate               force rate-view output (suppress per-file lines)
      --no-progress        suppress all progress output (implies quiet)
  -q, --quiet              suppress non-error output
  -v, --verbose            increase verbosity (show skipped files, checksum results)
      --log FILE           write structured JSON log to FILE in addition to terminal output
      --color              force color output even when not a TTY
      --no-color           disable color output

  # misc
      --benchmark          measure source read and dest write throughput before copying; auto-tune workers
      --version            print version and exit
```

### Invocation Examples

```bash
# local recursive copy, archive mode
beam -a ~/Downloads/photos /mnt/nas/photos

# remote copy with bandwidth limit and verification
beam --verify --bwlimit 200MB /data user@nas:/backup/data

# dry run with verbose output
beam --dry-run -v /src /dst

# large overnight transfer: full TUI
beam -a --verify --tui /Volumes/Archive /mnt/nas/archive

# piped / CI: no ANSI, clean output
beam -a /src /dst 2>&1 | tee transfer.log

# force per-file output even for many small files
beam --feed /workspace/node_modules /mnt/nas/backup/node_modules
```

---

## Inline Output Specification

This is the default output mode. Do not capture the terminal. Do not use alternate screen buffer.

### Feed Mode (few or large files, or `--feed`)

Each completed file prints one line and scrolls into terminal history:

```
✓  home-video/2019/raw/GH004812.MP4       4.2 GB    1.81 GB/s
✓  home-video/2019/raw/GH004813.MP4       3.7 GB    1.63 GB/s
–  photos/2023/london/DSC_0443.NEF       27.9 MB    unchanged
✗  home-video/2021/trips/GH004821.MP4    4.8 GB    permission denied
```

Format: `<icon>  <path padded>  <size right-aligned>  <speed or status>`

Icons: `✓` (done, green), `–` (skipped, dim), `✗` (error, red)

Below the scrolling lines, a 2-line HUD rewrites in place:

```
 27%  ████████░░░░░░░░░░░░░░░░░░░░  84.2 / 312.7 GB
      1.61 GB/s   14,302 / 48,917 files   eta 23m 41s   ▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪□□□□□□□□ 24 workers
```

The worker indicator is a row of Unicode block characters: filled = busy, empty = idle, dim-filled = stalled.

HUD rewrite implementation:
```go
// On each tick, move cursor up N lines, clear to bottom, redraw
fmt.Fprintf(w, "\033[%dA\033[J", hudLines)
fmt.Fprint(w, hudContent)
```

On the first render (no previous HUD), just print. Track `hudLines` as a struct field.

### Rate Mode (many small files, or `--rate`)

Automatically activated when files/sec exceeds threshold (default 200/s, measured over a 2s window). Prints a one-time notice to stderr, then switches the HUD:

```
↯  rate view  (642 files/s · --feed to see individual files)

files/s  ▁▂▃▄▅▆▇█▇▆▅▄▅▆▇█▇▆▅▆  642 files/s   21,402 / 48,917 done
 18%  ████░░░░░░░░░░░░░░░░░░░░░░░░  0.38 / 2.1 GB
      284 MB/s   21,402 / 48,917 files   eta 4m 12s   ▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪▪□□□□□□□□ 24 workers
```

The sparkline is 20 characters wide using Unicode Braille or block-element characters representing the last 20 seconds of files/sec.

### Completion

HUD is replaced by a single summary line:

```
✓  48,917 files   2.1 GB   641 MB/s avg   3m 17s   0 errors
```

If errors occurred:
```
✓  14,309 files   312.7 GB   1.61 GB/s avg   3h 14m 22s   1 error  (beam --errors <job-id> to review)
```

### Non-TTY / Piped Mode

Detect via `term.IsTerminal(os.Stdout.Fd())`. When not a TTY:
- No ANSI escape sequences
- No cursor movement
- Each file prints a newline-terminated plain line
- Progress is emitted as a periodic plain-text line to stderr (every 5s): `progress: 27% 84.2/312.7GB 14302/48917 files 1.61GB/s eta 23m41s`
- Summary is a plain line to stdout

---

## Full TUI Specification (`--tui`)

Implemented using Bubble Tea. Activated with `--tui` flag only.

### Feed View (default TUI view)

Three sections separated by dim ASCII dividers:
1. **In flight** — files currently being transferred, each with a 2px inline mini-bar
2. **Recently completed** — last N completed files (N = terminal height ÷ 3), scrolls
3. **Errors** — all errors, never scroll off screen

Header: stat bar with transferred, total, files done/total, ETA, worker count.
Footer: keybind strip (`p` pause, `+`/`-` workers, `v` verify toggle, `r` rate view, `e` errors, `q` quit).

### Rate View (TUI, press `r`)

- Large throughput number (current GB/s or MB/s) prominently displayed
- 60-second sparkline (bar chart, full width of terminal)
- Files/sec + bytes/sec cells
- Worker grid: one colored square per worker, busy = blue, idle = dim, stalled = yellow

### Theme

Use the Catppuccin Mocha palette (already familiar from the mockups):
```go
var (
  Green  = lipgloss.Color("#a6e3a1")
  Blue   = lipgloss.Color("#89b4fa")
  Yellow = lipgloss.Color("#f9e2af")
  Red    = lipgloss.Color("#f38ba8")
  Teal   = lipgloss.Color("#94e2d5")
  Mauve  = lipgloss.Color("#cba6f7")
  Muted  = lipgloss.Color("#5a6278")
  Dim    = lipgloss.Color("#3a4055")
  Bright = lipgloss.Color("#cdd6f4")
)
```

---

## Stats Collection

Use a dedicated `Collector` struct backed entirely by `sync/atomic` — no mutexes on the hot path.

```go
type Collector struct {
  BytesCopied    atomic.Int64
  BytesTotal     atomic.Int64
  FilesDone      atomic.Int64
  FilesTotal     atomic.Int64
  FilesSkipped   atomic.Int64
  FilesErrored   atomic.Int64
  StartTime      time.Time

  // throughput ring buffer: 1 sample/sec, 60 samples
  mu          sync.Mutex
  throughput  [60]int64   // bytes/sec per second
  tIdx        int
}
```

A background goroutine samples `BytesCopied` once per second, computes the delta, and writes to the ring buffer. The UI reads from this ring buffer to render sparklines and current speed.

---

## Testing Strategy

### Unit Tests
- `transport/delta_test.go` — verify delta algorithm produces correct output for known inputs; test block matching edge cases (all-same, all-different, partial match)
- `engine/worker_test.go` — test atomic write behavior, tmp file cleanup on error, sparse file reproduction
- `filter/rules_test.go` — test include/exclude glob matching against rsync's behavior for compatibility
- `platform/copy_test.go` — test fast path selection logic and fallback behavior
- `stats/collector_test.go` — verify ring buffer, throughput calculation, race-free under `go test -race`

### Integration Tests
- Copy a directory tree, verify checksums match source
- Copy with `--delete`, verify destination is exactly the source
- Interrupt mid-copy, resume, verify result is correct
- Copy a file with holes, verify hole structure preserved
- Copy hardlinked files, verify hardlinks reproduced

### Benchmarks
- `BenchmarkLocalCopy` — compare beam vs `cp -r` vs `rsync` on synthetic datasets: 1 large file, 1000 small files, 10000 small files
- `BenchmarkDelta` — rolling hash throughput in GB/s
- `BenchmarkChecksum` — BLAKE3 throughput vs SHA256 vs MD5

---

## Project Structure

```
beam/
├── cmd/beam/main.go
├── internal/
│   ├── engine/
│   ├── platform/
│   ├── transport/
│   ├── ui/
│   │   ├── inline.go
│   │   └── tui/
│   ├── stats/
│   └── filter/
├── go.mod
├── go.sum
├── Makefile
├── README.md
└── .github/
    └── workflows/
        ├── ci.yml       (go test ./..., go vet, staticcheck)
        └── release.yml  (goreleaser: linux/amd64, linux/arm64, darwin/amd64, darwin/arm64)
```

### Makefile targets

```makefile
build:    go build -ldflags="-s -w" -o bin/beam ./cmd/beam
test:     go test -race ./...
bench:    go test -bench=. -benchmem ./...
lint:     staticcheck ./... && golangci-lint run
release:  goreleaser release --clean
```

---

## Implementation Order

Build in this sequence. Each phase is independently useful:

1. **Phase 1 — Core engine, local copy only**
   - Directory scanner with goroutine pool
   - Worker pool with atomic writes
   - Platform fast paths (copy_file_range, clonefile, fallback)
   - Sparse file support
   - Hardlink tracking

2. **Phase 2 — Inline UI**
   - Non-TTY plain output
   - TTY feed mode with 2-line HUD
   - Rate mode with sparkline + adaptive switching
   - Completion summary

3. **Phase 3 — Filtering, delete, dry-run**
   - Glob include/exclude rules
   - `--delete` pass
   - `--dry-run` diff output

4. **Phase 4 — Verify + mtime skip**
   - Post-copy BLAKE3 verification pass
   - Mtime-based skip detection (same size + modtime = skip)

5. **Phase 5 — Full TUI + multi-source**
   - Bubble Tea model
   - Feed view, rate view, error panel
   - Keybinds, pause/resume, worker adjustment
   - Multi-source support (`beam -a src1 src2 dst`)
   - Config system, worker throttle

6. **Phase 6a — Transport abstraction + SFTP + push delta**
   - `ReadEndpoint` / `WriteEndpoint` interfaces (`internal/transport`)
   - Local and SFTP endpoint implementations
   - Refactor scanner, worker, delete, verify to use endpoints
   - SSH connection management with auth chain (agent → keys → password)
   - `user@host:path` CLI syntax parsing
   - Push-only delta transfer (xxHash + BLAKE3 block matching)

7. **Phase 6b — Custom binary protocol** (complete)
   - Standalone TCP daemon (`beam daemon`) serving files from a root directory
   - msgpack wire format via `tinylib/msgp` (zero-alloc, map encoding for backward compat)
   - Stream multiplexer over single TLS connection
   - TLS + pre-shared bearer token authentication
   - `beam://[token@]host[:port]/path` URL scheme
   - Server-side BLAKE3 hashing (NativeHash capability)
   - `BeamReadEndpoint` + `BeamWriteEndpoint` implementing transport interfaces

8. **Phase 6c — Server-side delta transfer** (complete)
   - Three new RPCs: ComputeSignature, MatchBlocks, ApplyDelta
   - Push-delta (local→beam): sigs computed on dst daemon, ops applied on dst daemon
   - Pull-delta (beam→local): sigs computed locally, matching done on src daemon
   - Beam-to-beam delta: client relays sigs/ops between daemons
   - `DeltaTransfer` capability signaling in handshake
   - Copy direction defines roles — no separate negotiation protocol needed

9. **Phase 6d — SSH beam auto-detection** (future)
   - Auto-detect beam on remote: try `beam --server` over SSH, fall back to SFTP
   - SSH subsystem or remote shell execution for beam protocol bootstrapping

10. **Phase 7 — Polish**
   - `--benchmark` mode
   - `--bwlimit` rate limiting (token bucket)
   - Structured JSON logging
   - Man page generation (`cobra` has built-in support)
   - goreleaser config, GitHub Actions CI/CD

---

## Non-Goals (explicitly out of scope for v1)

- Config file of any kind
- Daemon mode
- GUI
- Windows support (target Linux and macOS only for v1; Windows is a future consideration)
- S3 / object storage transport (design the transport interface to make this addable later)
- Encryption at rest (transport layer uses TLS; not a backup tool)

---

## Reference Material

Before implementing, familiarize yourself with:
- `man 2 copy_file_range` — Linux kernel copy offload
- `man 2 clonefile` — macOS APFS copy-on-write
- `man 2 lseek` with `SEEK_DATA`/`SEEK_HOLE` — sparse file detection
- The rsync algorithm paper: https://www.andrew.cmu.edu/course/15-749/READINGS/required/cas/tridgell96.pdf
- Bubble Tea docs: https://github.com/charmbracelet/bubbletea
- goreleaser: https://goreleaser.com

---

## Success Criteria

- `beam` copies a 10GB file at least as fast as `cp` on Linux (limited by disk, not beam overhead)
- `beam` copies 50,000 small files faster than `rsync` with equivalent flags
- Inline output renders correctly in: iTerm2, Terminal.app, GNOME Terminal, tmux, zsh, bash
- Non-TTY output is clean, greppable, and produces no ANSI artifacts
- `beam` exits with code 0 on success, 1 on partial failure (some errors), 2 on total failure
- `go test -race ./...` passes with zero races

---

## Current Status

### Completed
- **Phase 1** — Core engine with local copy (PR #1)
- **Phase 2+3** — Event system, inline UI, filtering, and delete (PR #2)
- **Phase 4** — BLAKE3 post-copy verification, mtime-based skip detection (PR #3)
- **Phase 5** — TUI overhaul, config system, worker throttle, multi-source support (PR #4, #5)
- **Phase 6a** — Transport abstraction, SFTP endpoints, SSH auth, push-only delta transfer (PR #6)
- **Phase 6b** — Custom binary protocol with standalone TCP daemon, `beam://` URL scheme, msgpack wire format, TLS+bearer auth, stream multiplexer, server-side hashing (PR #7)
- **Phase 6c** — Server-side delta transfer: ComputeSignature/MatchBlocks/ApplyDelta RPCs, push-delta, pull-delta, beam-to-beam delta, DeltaTransfer capability flag

### Next Up
- **Phase 6d** — SSH beam auto-detection
  - Auto-detect beam on remote: try `beam --server` over SSH, fall back to SFTP
