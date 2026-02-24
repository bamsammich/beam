---
status: accepted
date: 2026-02-24
---

# Platform-specific kernel copy offload

## Context and Problem Statement

Userspace read/write loops for file copying are limited by context-switch overhead and memory copies. Modern kernels offer zero-copy or near-zero-copy file copy syscalls that bypass userspace buffers entirely.

## Decision Drivers

* Local copy performance must beat rsync on the same hardware
* Must support Linux and macOS with graceful degradation
* io_uring available only on Linux kernel >= 5.6

## Considered Options

* Userspace read/write with large aligned buffers (portable)
* Platform-specific syscalls: `copy_file_range` + `io_uring` (Linux), `clonefile` (Darwin)
* `sendfile` only

## Decision Outcome

Chosen option: "Platform-specific syscalls with userspace fallback", because kernel-level copy avoids crossing the user/kernel boundary per block. Build tags (`copy_linux.go`, `copy_darwin.go`, `copy_fallback.go`) select the implementation at compile time. On Linux: `copy_file_range(2)` with optional io_uring (runtime-detected). On Darwin: `clonefile(2)` for APFS instant copies. Fallback: large aligned buffer read/write.

### Consequences

* Good, because local copies are 2x+ faster than rsync on tested hardware
* Good, because APFS clonefile gives instant copy-on-write when source and dest are on the same volume
* Bad, because `--bwlimit` has no effect on local copies — kernel fast paths bypass the userspace rate limiter
