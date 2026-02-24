---
status: accepted
date: 2026-02-24
---

# Atomic writes via temp file and rename

## Context and Problem Statement

File copy tools risk leaving partial or corrupted files at the destination if interrupted by crash, Ctrl-C, or power loss. beam needs a strategy that guarantees destination files are always complete or absent, never partial.

## Decision Drivers

* Interrupted copies must never leave corrupt files
* Workers run in parallel — concurrent writes to the same directory must be safe
* Must work across ext4, APFS, ZFS, and tmpfs

## Considered Options

* Write directly to destination path
* Write to temp file (`<dest>.<uuid>.beam-tmp`) then `os.Rename`
* Write to temp directory then move

## Decision Outcome

Chosen option: "Write to temp file then rename", because `os.Rename` is atomic on POSIX filesystems when source and destination are on the same device. Writing to `<dest>.<uuid>.beam-tmp` in the same directory guarantees same-device rename. UUID suffix prevents collisions between parallel workers.

### Consequences

* Good, because crash/Ctrl-C never leaves a partial file at the destination path
* Good, because cleanup on interrupt only needs to remove `.beam-tmp` files
* Bad, because requires extra disk space for the temp file during copy (up to 2x for the largest file)
