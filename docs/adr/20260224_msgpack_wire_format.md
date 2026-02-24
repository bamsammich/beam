---
status: accepted
date: 2026-02-24
---

# Custom binary protocol with msgpack map encoding

## Context and Problem Statement

beam needs a wire protocol for its daemon that is faster than SSH/SFTP for bulk transfers, supports server-side operations (hashing, delta), and can evolve without breaking older clients. The protocol must handle streaming file data alongside RPC-style request/response messages.

## Decision Drivers

* Must be faster than SFTP for both large files and many small files
* Backward compatibility is critical — old clients must work with new servers
* Code-generation preferred for zero-allocation marshaling

## Considered Options

* Protocol Buffers (protobuf)
* msgpack with positional array encoding
* msgpack with map encoding (string keys)

## Decision Outcome

Chosen option: "msgpack with map encoding via tinylib/msgp code-gen", because map encoding uses string field names as keys, allowing old clients to ignore unknown fields and new servers to handle missing fields with zero-value defaults. This is the **strict backward compatibility rule**: new fields must be additive, fields must never be removed or renamed. `go generate` produces zero-allocation marshal/unmarshal code. The protocol uses `beam://[token@]host[:port]/path` URL scheme with TLS + bearer token authentication.

### Consequences

* Good, because protocol can evolve without version negotiation or feature flags
* Good, because tinylib/msgp generates zero-alloc code competitive with protobuf
* Bad, because map encoding has ~30% wire overhead vs positional encoding due to string keys
