---
status: accepted
date: 2026-02-24
---

# Parallel scanner-batcher-worker pipeline

## Context and Problem Statement

beam must efficiently copy directory trees that may contain millions of files ranging from tiny configs to multi-gigabyte media. A single-threaded approach would underutilize modern multi-core systems and fast storage (NVMe, RAID).

## Decision Drivers

* Must saturate NVMe/SSD I/O bandwidth on local copies
* Small files are bottlenecked by per-file overhead, not throughput
* Pipeline stages must be independently scalable

## Considered Options

* Single-threaded sequential scan-then-copy
* Parallel scanner feeding a worker pool via buffered channel
* Work-stealing queue

## Decision Outcome

Chosen option: "Parallel scanner feeding worker pool via buffered channel", because Go channels provide natural backpressure (buffer size = 2x workers) and goroutines are cheap. The pipeline is: scanner goroutine pool → optional batcher (beam:// only, coalesces small files < 64KB into batch RPCs) → worker goroutine pool → optional verify goroutine pool. Each stage runs concurrently and communicates via typed channels.

### Consequences

* Good, because adding the batcher stage was a non-breaking extension to the existing pipeline
* Good, because worker count is adjustable at runtime via atomic.Int32 (TUI +/- keys)
* Bad, because prescan phase blocks before copy starts to compute accurate totals for progress display
