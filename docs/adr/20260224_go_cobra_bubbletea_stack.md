---
status: accepted
date: 2026-02-24
---

# Use Go with Cobra CLI and Bubble Tea TUI

## Context and Problem Statement

beam needs a language and framework stack for a high-performance file copy tool that provides both a CLI and an optional full-screen TUI. The tool must support platform-specific syscalls, parallel goroutine-based workers, and cross-compile to Linux and macOS.

## Decision Drivers

* Must support low-level syscalls (copy_file_range, io_uring, clonefile) without FFI overhead
* Need lightweight concurrency for parallel scanning and copying
* Need a rich terminal UI framework for the optional `--tui` mode
* Single static binary distribution preferred

## Considered Options

* Go + Cobra + Bubble Tea
* Rust + clap + ratatui
* Python + click + rich

## Decision Outcome

Chosen option: "Go + Cobra + Bubble Tea", because Go provides goroutine-based parallelism with direct syscall access, compiles to a single static binary, and the Charm ecosystem (Bubble Tea + Lip Gloss) is the most mature Go TUI framework. Cobra is the de facto standard for Go CLIs.

### Consequences

* Good, because goroutines map naturally to scanner/worker pool architecture with minimal overhead
* Good, because single-binary distribution simplifies deployment (no runtime dependencies)
* Good, because Bubble Tea's Elm architecture keeps TUI state management clean
* Bad, because Go's error handling is verbose compared to Rust's `Result` type
