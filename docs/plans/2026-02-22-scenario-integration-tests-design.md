# Scenario-Based Integration Tests Design

**Goal:** Add integration tests for 5 engine features that lack coverage: `--delete`, interrupt/resume, sparse files, hardlinks, and `--verify`.

**Architecture:** All tests run local-to-local via `engine.Run`, following the existing integration test pattern. No containers or network transports needed — the scenarios test engine logic that is transport-agnostic.

**Tech Stack:** Go stdlib `testing`, `testify/require`, `syscall`, `golang.org/x/sys/unix`

---

## Test Scenarios

### 1. TestIntegration_Delete

**Setup:**
- Create source tree with `createTestTree` (root.txt, big.bin, sub/mid.txt, sub/deep/leaf.txt, link.txt)
- Pre-populate destination with extra files: `extra.txt` at root, `sub/orphan.txt` in subdirectory, `stale/` empty directory

**Run:** `engine.Run` with `Delete: true`, `Archive: true`, `Recursive: true`

**Verify:**
- `verifyTreeCopy(t, srcDir, dstDir)` — all source files present
- `extra.txt` does NOT exist in destination
- `sub/orphan.txt` does NOT exist in destination
- `stale/` directory does NOT exist in destination
- `result.Err` is nil

### 2. TestIntegration_InterruptAndResume

**Setup:**
- Create source tree with `createTestTree`

**Phase 1 — Interrupt:**
- Create event channel that monitors for `event.FileCompleted`
- After first `FileCompleted`, cancel the context
- Run `engine.Run` with cancellable context, `Workers: 1` (serializes file processing for deterministic cancellation)
- Verify: no `.beam-tmp` files exist in destination (atomic write cleanup)
- Verify: any files that DO exist in destination have correct content (byte-for-byte match with source)

**Phase 2 — Resume:**
- Re-run `engine.Run` with fresh context (no cancellation), same src/dst
- Verify: `verifyTreeCopy(t, srcDir, dstDir)` — complete copy
- Verify: `result.Stats.FilesSkipped > 0` — previously completed files were skipped via mtime detection

### 3. TestIntegration_SparseFile

**Setup:**
- Create a sparse file in source directory:
  - Write 4KB of data at offset 0
  - Seek to offset 1MB, write 4KB of data
  - Total apparent size: ~1MB + 4KB, actual disk blocks: ~8KB
- Also include a normal file for baseline

**Run:** `engine.Run` with `Archive: true`

**Verify:**
- Destination file has same apparent size as source (`os.Stat().Size()`)
- Destination file content matches source (read both, compare bytes)
- Destination file has sparse structure: use `SEEK_DATA`/`SEEK_HOLE` to verify hole exists between the two data regions
- Alternatively: compare `stat.Blocks` — destination blocks should be similar to source blocks (both sparse), not fully materialized

### 4. TestIntegration_Hardlinks

**Setup:**
- Create directory with:
  - `original.txt` (regular file with content)
  - `hardlink.txt` → hardlink to `original.txt` (via `os.Link`)
  - `sub/another.txt` (regular file, different content)

**Run:** `engine.Run` with `Archive: true`, `Recursive: true`

**Verify:**
- Both `original.txt` and `hardlink.txt` exist in destination
- Content matches source for both files
- `os.Stat` on both destination files: same `Ino` (inode number) from `syscall.Stat_t`
- `sub/another.txt` has different inode from the hardlinked pair

### 5. TestIntegration_Verify

**Phase 1 — Verify passes:**
- Create source tree, run `engine.Run` with `Verify: true`
- Collect events via channel, look for `event.VerifyOK` events
- `result.Err` is nil (all checksums match)

**Phase 2 — Verify detects corruption:**
- Corrupt a destination file (overwrite first byte of `root.txt`)
- Re-run `engine.Run` with `Verify: true` from same source to same destination
- Note: skip detection will see different size/mtime and re-copy the file, fixing the corruption
- Better approach: run copy first (without verify), then corrupt, then run verify-only pass via `engine.Verify()` directly
- Verify: `result.Failed > 0`
- Collect events, verify `event.VerifyFailed` emitted for corrupted file

## File Changes

| File | Action |
|------|--------|
| `internal/engine/integration_test.go` | MODIFY — add 5 new test functions |
| `internal/engine/testhelpers_test.go` | MODIFY — add `createSparseFile`, `createHardlinkTree` helpers |

## Helper Functions (testhelpers_test.go)

**`createSparseFile(t, path string, dataSize, holeSize int64)`**
- Write `dataSize` bytes of data, seek forward `holeSize`, write `dataSize` more bytes
- Returns apparent file size

**`createHardlinkTree(t, root string)`**
- Creates `original.txt` with known content
- `os.Link(original.txt, hardlink.txt)`
- Creates `sub/another.txt` with different content

## Key Constraints

- **Sparse tests are filesystem-dependent**: tmpfs may or may not preserve holes. ext4 and xfs do. The test should skip gracefully if `SEEK_DATA` returns `EINVAL`.
- **Hardlink tests require same device**: `t.TempDir()` returns paths on the same filesystem, so `os.Link` will work.
- **Interrupt test uses Workers: 1** to serialize processing and make cancellation timing predictable.
- **Verify corruption test** calls `engine.Verify()` directly rather than `engine.Run()` to avoid skip detection re-copying the file.
