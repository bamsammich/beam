# Phase 6c: Server-Side Delta Transfer for Beam Protocol

## Scope

Three new RPCs on the beam daemon enable push-delta, pull-delta, and beam-to-beam delta with minimal wire transfer. The copy direction (src->dst) naturally determines which daemon computes signatures, which matches blocks, and which applies the delta — no separate negotiation protocol needed.

SFTP remains a full-stream-copy fallback (it has no server-side compute).

### Out of scope (deferred to Phase 6d)

- **Auto-detect beam on remote** — try `beam --server` over SSH, fall back to SFTP

## Problem

The current push-delta in `copyDataDelta()` streams the old destination file over the wire twice (once for signature computation, once as basis for ApplyDelta) and writes the full reconstructed file back. For a beam endpoint, this is 3x the file size over the wire — worse than a single full-file transfer.

For the pull direction (beam src -> local dst), no delta path exists at all — the full file streams every time.

For beam-to-beam, no delta exists at all.

## Design

### Protocol Messages

Six new message types in `proto/messages.go`:

| Message | Code | Fields |
|---------|------|--------|
| `ComputeSignatureReq` | 0x50 | `RelPath string`, `FileSize int64` |
| `ComputeSignatureResp` | 0x51 | `BlockSize int`, `Signatures []BlockSignature` |
| `MatchBlocksReq` | 0x52 | `RelPath string`, `BlockSize int`, `Signatures []BlockSignature` |
| `MatchBlocksResp` | 0x53 | `Ops []DeltaOp` |
| `ApplyDeltaReq` | 0x54 | `BasisRelPath string`, `TempRelPath string`, `Ops []DeltaOp` |
| `ApplyDeltaResp` | 0x55 | `BytesWritten int64` |

`BlockSignature` and `DeltaOp` gain msgpack tags for wire serialization. These types are defined in `transport/delta.go` but their msgpack-generated code lives in `proto/`.

### Capability Signaling

- `HandshakeResp.Capabilities` string list includes `"delta-transfer"` when the daemon supports the three new RPCs
- `transport.Capabilities` struct gains `DeltaTransfer bool`
- Beam endpoint constructors set `DeltaTransfer` based on handshake capabilities
- Client checks capability before attempting delta RPCs; falls through to full stream on older daemons

### Delta Roles

The rsync algorithm assigns fixed roles based on data direction:

- **Receiver** (dst, has old file): computes signatures, applies delta
- **Sender** (src, has new file): matches blocks against signatures

The beam client orchestrates by calling the appropriate RPC on each daemon. The copy direction IS the negotiation.

### Push Delta Flow (local src -> beam dst)

1. `ComputeSignature(relDst)` RPC on **dst daemon** -> sigs of old dest
2. `MatchBlocks(localSrc, sigs)` **locally** -> delta ops
3. `CreateTemp(relDst)` RPC on **dst daemon** -> temp file path
4. `ApplyDelta(relDst, tempPath, ops)` RPC on **dst daemon** -> reconstruct
5. `Rename(tempPath, relDst)` RPC on **dst daemon**

Wire: sigs (dst->client, small) + ops (client->dst, small).

### Pull Delta Flow (beam src -> local dst)

1. `ComputeSignature(localOldDest)` **locally** -> sigs
2. `MatchBlocks(relSrc, sigs)` RPC on **src daemon** -> delta ops
3. `ApplyDelta(localOldDest, ops, localTmpFile)` **locally** -> reconstruct
4. Local rename

Wire: sigs (client->src, small) + ops (src->client, small).

### Beam-to-Beam Delta Flow (beam src -> beam dst)

Client relays between the two daemons:

1. `ComputeSignature(relDst)` RPC on **dst daemon** -> sigs
2. `MatchBlocks(relSrc, sigs)` RPC on **src daemon** -> delta ops
3. `CreateTemp(relDst)` RPC on **dst daemon** -> temp file path
4. `ApplyDelta(relDst, tempPath, ops)` RPC on **dst daemon** -> reconstruct
5. `Rename(tempPath, relDst)` RPC on **dst daemon**

Wire: sigs (dst->client->src) + ops (src->client->dst). Client is a relay.

### Worker Integration

`copyFileData()` in `worker.go` gains new code paths, ordered by preference:

1. **Beam push-delta**: dst is `BeamWriteEndpoint` with `DeltaTransfer` cap, basis exists on dst
2. **Beam pull-delta**: src is `BeamReadEndpoint` with `DeltaTransfer` cap, old dest exists locally
3. **Beam-to-beam delta**: both endpoints are beam with `DeltaTransfer` cap, basis exists on dst
4. **Legacy push-delta**: existing `copyDataDelta()` for SFTP or non-delta-capable endpoints
5. **Full stream copy**: final fallback

Each delta path falls through to the next on error.

### Server-Side Handler

Three new dispatch cases in `handler.go`:

- `handleComputeSignature`: opens local file, calls `transport.ComputeSignature()`, returns sigs
- `handleMatchBlocks`: opens local file, calls `transport.MatchBlocks()` against provided sigs, returns ops
- `handleApplyDelta`: opens basis file + temp file, calls `transport.ApplyDelta()`, returns bytes written

### Testing

**Unit tests** (`proto/handler_test.go`):
- ComputeSignature returns correct block count for known file
- MatchBlocks detects identical blocks, different blocks, partial matches
- ApplyDelta produces correct output from basis + ops

**Integration tests** (`engine/integration_test.go`):
- `TestIntegration_LocalToBeam_Delta`: pre-populate beam dst with old version, copy updated src, verify correct result
- `TestIntegration_BeamToLocal_Delta`: pre-populate local dst with old version, pull from beam src, verify correct result
- `TestIntegration_BeamToBeam_Delta`: pre-populate beam dst with old version, copy from beam src, verify correct result
