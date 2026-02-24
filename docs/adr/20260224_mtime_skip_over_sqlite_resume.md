---
status: accepted
date: 2026-02-24
---

# Use mtime+size skip detection instead of SQLite checkpoint resume

## Context and Problem Statement

beam initially used SQLite to checkpoint completed files for crash-resume. This added a runtime dependency (modernc.org/sqlite, pure-Go), increased binary size, and required WAL-mode writes during copy. The question is whether the complexity is justified when simpler skip detection achieves the same practical goal.

## Decision Drivers

* Re-running beam on an interrupted copy should skip already-completed files
* SQLite added ~8MB to binary size and significant code complexity
* rsync's default skip heuristic (mtime + size) is well-understood and sufficient for most use cases

## Considered Options

* SQLite checkpoint database per job (implemented in phase 4, removed in phase 5)
* mtime + size match = skip (rsync default behavior)

## Decision Outcome

Chosen option: "mtime + size skip detection", because atomic writes (temp file → rename) guarantee that any file present at the destination is complete. If destination mtime and size match source, the file can be safely skipped. This eliminates the SQLite dependency entirely.

### Consequences

* Good, because binary size decreased ~8MB and checkpoint code was removed
* Good, because skip detection works across restarts without any state file
* Bad, because files modified without changing size or mtime won't be detected as changed (mitigated by `--verify` flag)
