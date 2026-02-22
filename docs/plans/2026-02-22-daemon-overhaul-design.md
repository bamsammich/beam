# Daemon Overhaul Design

**Goal:** Make `beam daemon` zero-config by auto-generating auth tokens, defaulting root to `/`, and writing a discovery file that SSH-bootstrapped clients can read.

**Motivation:** This is Phase A of a two-phase effort. Phase B (SSH-bootstrapped beam upgrade) will have clients read the discovery file over SFTP to opportunistically upgrade from SFTP to beam protocol. This phase prepares the daemon side.

---

## Token Discovery File

Written by the daemon on startup to `~/.config/beam/daemon.toml`:

```toml
token = "a1b2c3d4..."
port = 9876
```

- `token`: 32 random bytes, hex-encoded (`crypto/rand`)
- `port`: actual listening port (resolved from `:0` if random)
- Overwritten on every daemon start (implicit key rotation)
- Read by remote clients over SFTP in Phase B

## CLI Changes

```
beam daemon [flags]

  --root DIR       restrict filesystem access to DIR (default: /)
  --listen ADDR    listen address (default: :9876)
  --token TOKEN    use a specific token instead of auto-generating
  --tls-cert FILE  TLS certificate file (default: auto-generated self-signed)
  --tls-key FILE   TLS private key file
```

All flags optional. `beam daemon` with zero arguments is valid.

## Daemon Startup Flow

1. If `--token` not provided, generate token via `crypto/rand` (32 bytes, hex)
2. If `--root` not provided, default to `/`
3. Validate root exists and is a directory
4. Start TLS listener
5. Write `~/.config/beam/daemon.toml` with resolved `{token, port}`
6. Log: `beam daemon listening on :9876`
7. On shutdown, remove the discovery file (best-effort cleanup)

## Code Changes

### `internal/config/daemon.go` (new file)

```go
type DaemonDiscovery struct {
    Token string `toml:"token"`
    Port  int    `toml:"port"`
}

func DaemonDiscoveryPath() string        // ~/.config/beam/daemon.toml
func WriteDaemonDiscovery(d DaemonDiscovery) error
func ReadDaemonDiscovery() (DaemonDiscovery, error)
```

### `internal/transport/proto/daemon.go`

- `DaemonConfig.Root`: default to `/` if empty
- `DaemonConfig.AuthToken`: no longer required; caller generates if empty
- Remove root/token validation from `NewDaemon()` — caller handles defaults

### `cmd/beam/daemon.go`

- Remove `MarkFlagRequired("root")` and `MarkFlagRequired("token")`
- Change `--root` default to `/`
- Change `--listen` default to `:9876`
- Add token auto-generation when `--token` not provided
- Write discovery file after listener starts
- Register shutdown hook to remove discovery file

### Test helpers

- `startTestDaemon()` in `internal/engine/testhelpers_test.go` passes explicit token and root — no changes needed
- Add unit tests for `config.WriteDaemonDiscovery` / `ReadDaemonDiscovery`

## What Doesn't Change

- Protocol messages (all use relative paths)
- Handler path resolution (`filepath.Join(root, relPath)`)
- TLS (self-signed default, optional custom certs)
- Client-side `beam://` URL parsing
- Existing integration tests (they pass explicit token/root)
