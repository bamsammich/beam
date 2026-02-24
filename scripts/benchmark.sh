#!/usr/bin/env bash
#
# benchmark.sh — beam vs rsync comparative benchmarks
#
# Generates test data (1 GB file + 10,000 x 4 KB files) and runs beam vs rsync
# across local, beam://, and beam-over-SSH scenarios using hyperfine.
# Outputs machine-readable JSON for updating README tables.
#
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

RUNS=3
WARMUP=1
SKIP_LOCAL=false
SKIP_REMOTE=false
OUTPUT=""
BEAM="./bin/beam"
DATA_DIR=""
DAEMON_PORT=19876
DAEMON_PID=""
DAEMON_TOKEN=""

# ── Usage ─────────────────────────────────────────────────────────────────────

usage() {
    cat <<'EOF'
Usage: scripts/benchmark.sh [OPTIONS]

Run beam vs rsync comparative benchmarks using hyperfine.

Options:
  --runs N          Number of benchmark runs (default: 3)
  --warmup N        Warmup runs before measuring (default: 1)
  --skip-local      Skip local transfer benchmarks
  --skip-remote     Skip remote (beam:// + SSH) transfer benchmarks
  --output FILE     JSON output file (default: stdout)
  --beam PATH       Path to beam binary (default: ./bin/beam)
  --data-dir PATH   Directory for test data (default: auto-created tmpdir, cleaned up)
  -h, --help        Show this help message

Examples:
  # Quick local-only test
  scripts/benchmark.sh --skip-remote --runs 1 --warmup 0

  # Full run with JSON output
  scripts/benchmark.sh --output /tmp/bench.json --runs 3

  # Use a specific beam binary
  scripts/benchmark.sh --beam /usr/local/bin/beam
EOF
    exit 0
}

# ── Argument parsing ──────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs)      RUNS="$2"; shift 2 ;;
        --warmup)    WARMUP="$2"; shift 2 ;;
        --skip-local)  SKIP_LOCAL=true; shift ;;
        --skip-remote) SKIP_REMOTE=true; shift ;;
        --output)    OUTPUT="$2"; shift 2 ;;
        --beam)      BEAM="$2"; shift 2 ;;
        --data-dir)  DATA_DIR="$2"; shift 2 ;;
        -h|--help)   usage ;;
        *)           echo "Unknown option: $1" >&2; usage ;;
    esac
done

# ── Dependency checks ────────────────────────────────────────────────────────

check_deps() {
    local missing=()
    for cmd in hyperfine rsync jq openssl; do
        if ! command -v "$cmd" &>/dev/null; then
            missing+=("$cmd")
        fi
    done
    if [[ ! -x "$BEAM" ]]; then
        echo "Error: beam binary not found at '$BEAM' (run 'make build' first)" >&2
        exit 1
    fi
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "Error: missing required tools: ${missing[*]}" >&2
        echo "Install with: mise install (for hyperfine) or your package manager" >&2
        exit 1
    fi
}

# ── Cleanup ───────────────────────────────────────────────────────────────────

AUTO_TMPDIR=""

cleanup() {
    stop_daemon
    if [[ -n "$AUTO_TMPDIR" && -d "$AUTO_TMPDIR" ]]; then
        rm -rf "$AUTO_TMPDIR"
    fi
}
trap cleanup EXIT

# ── Daemon lifecycle ──────────────────────────────────────────────────────────

start_daemon() {
    local root="$1"
    DAEMON_TOKEN=$(openssl rand -hex 16)

    "$BEAM" daemon --listen ":${DAEMON_PORT}" --root "$root" --token "$DAEMON_TOKEN" &>/dev/null &
    DAEMON_PID=$!

    # Wait for daemon to accept TCP connections
    local ready=false
    for _ in $(seq 1 40); do
        if bash -c "echo >/dev/tcp/localhost/${DAEMON_PORT}" 2>/dev/null; then
            ready=true
            break
        fi
        sleep 0.25
    done

    if [[ "$ready" != "true" ]]; then
        echo "Error: beam daemon failed to start on port $DAEMON_PORT" >&2
        exit 1
    fi
    echo "Daemon started (pid=$DAEMON_PID, port=$DAEMON_PORT)" >&2
}

stop_daemon() {
    if [[ -n "$DAEMON_PID" ]]; then
        kill "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
        DAEMON_PID=""
        echo "Daemon stopped" >&2
    fi
}

# ── Test data creation ────────────────────────────────────────────────────────

create_test_data() {
    local dir="$1"
    echo "Creating test data in $dir ..." >&2

    # 1 GB file (urandom so compression doesn't skew results)
    echo "  Generating 1 GB file ..." >&2
    dd if=/dev/urandom of="$dir/big.bin" bs=1M count=1024 status=progress 2>&1 | tail -1 >&2

    # 10,000 x 4 KB files
    echo "  Generating 10,000 x 4 KB files ..." >&2
    mkdir -p "$dir/smallfiles"
    for i in $(seq 1 10000); do
        dd if=/dev/urandom of="$dir/smallfiles/file_$(printf '%05d' "$i").dat" bs=4096 count=1 2>/dev/null
    done

    # Verify
    local big_size small_count
    big_size=$(stat -c %s "$dir/big.bin" 2>/dev/null || stat -f %z "$dir/big.bin")
    small_count=$(find "$dir/smallfiles" -type f | wc -l)
    echo "  big.bin: $(( big_size / 1048576 )) MB" >&2
    echo "  smallfiles: $small_count files" >&2
}

# ── Hyperfine runner ──────────────────────────────────────────────────────────

run_bench() {
    local name="$1" json_out="$2" prepare="$3"
    shift 3
    # Remaining args are alternating -n NAME COMMAND pairs
    echo "Running benchmark: $name ..." >&2
    hyperfine \
        --runs "$RUNS" \
        --warmup "$WARMUP" \
        --prepare "$prepare" \
        --export-json "$json_out" \
        "$@"
}

# ── JSON aggregation ──────────────────────────────────────────────────────────

# Extract stats for a named command from a hyperfine JSON file
extract_stats() {
    local json_file="$1" cmd_name="$2"
    jq --arg name "$cmd_name" '
        .results[] | select(.command | contains($name)) |
        { mean, stddev, min, max }
    ' "$json_file"
}

# Build the final aggregated JSON
aggregate_results() {
    local json_dir="$1"
    local beam_version go_version os_name arch_name

    beam_version=$("$BEAM" --version 2>&1 | head -1 || echo "unknown")
    go_version=$(go version | awk '{print $3}')
    os_name=$(uname -s | tr '[:upper:]' '[:lower:]')
    arch_name=$(uname -m)

    # Build JSON with jq
    local result
    result=$(jq -n \
        --arg date "$(date +%Y-%m-%d)" \
        --arg beam_version "$beam_version" \
        --arg go_version "$go_version" \
        --arg os "$os_name" \
        --arg arch "$arch_name" \
        --argjson runs "$RUNS" \
        --argjson warmup "$WARMUP" \
        '{
            metadata: {
                date: $date,
                beam_version: $beam_version,
                go_version: $go_version,
                os: $os,
                arch: $arch,
                runs: $runs,
                warmup: $warmup
            }
        }')

    # Add local results if they exist
    if [[ -f "$json_dir/local-1gb.json" ]]; then
        local beam_1gb rsync_1gb beam_10k rsync_10k
        beam_1gb=$(extract_stats "$json_dir/local-1gb.json" "beam")
        rsync_1gb=$(extract_stats "$json_dir/local-1gb.json" "rsync")
        beam_10k=$(extract_stats "$json_dir/local-10k.json" "beam")
        rsync_10k=$(extract_stats "$json_dir/local-10k.json" "rsync")

        result=$(echo "$result" | jq \
            --argjson beam_1gb "$beam_1gb" \
            --argjson rsync_1gb "$rsync_1gb" \
            --argjson beam_10k "$beam_10k" \
            --argjson rsync_10k "$rsync_10k" \
            '.local = {
                "1gb": {
                    beam: $beam_1gb,
                    rsync: $rsync_1gb,
                    speedup: (($rsync_1gb.mean / $beam_1gb.mean * 100 | round) / 100)
                },
                "10k": {
                    beam: $beam_10k,
                    rsync: $rsync_10k,
                    speedup: (($rsync_10k.mean / $beam_10k.mean * 100 | round) / 100)
                }
            }')
    fi

    # Add beam protocol results if they exist
    if [[ -f "$json_dir/beam-1gb.json" ]]; then
        local beam_1gb rsync_1gb beam_10k rsync_10k
        beam_1gb=$(extract_stats "$json_dir/beam-1gb.json" "beam")
        rsync_1gb=$(extract_stats "$json_dir/beam-1gb.json" "rsync")
        beam_10k=$(extract_stats "$json_dir/beam-10k.json" "beam")
        rsync_10k=$(extract_stats "$json_dir/beam-10k.json" "rsync")

        result=$(echo "$result" | jq \
            --argjson beam_1gb "$beam_1gb" \
            --argjson rsync_1gb "$rsync_1gb" \
            --argjson beam_10k "$beam_10k" \
            --argjson rsync_10k "$rsync_10k" \
            '.beam_protocol = {
                "1gb": {
                    beam: $beam_1gb,
                    rsync: $rsync_1gb,
                    speedup: (($rsync_1gb.mean / $beam_1gb.mean * 100 | round) / 100)
                },
                "10k": {
                    beam: $beam_10k,
                    rsync: $rsync_10k,
                    speedup: (($rsync_10k.mean / $beam_10k.mean * 100 | round) / 100)
                }
            }')
    fi

    # Add beam-over-SSH results if they exist
    if [[ -f "$json_dir/ssh-1gb.json" ]]; then
        local beam_1gb rsync_1gb beam_10k rsync_10k
        beam_1gb=$(extract_stats "$json_dir/ssh-1gb.json" "beam")
        rsync_1gb=$(extract_stats "$json_dir/ssh-1gb.json" "rsync")
        beam_10k=$(extract_stats "$json_dir/ssh-10k.json" "beam")
        rsync_10k=$(extract_stats "$json_dir/ssh-10k.json" "rsync")

        result=$(echo "$result" | jq \
            --argjson beam_1gb "$beam_1gb" \
            --argjson rsync_1gb "$rsync_1gb" \
            --argjson beam_10k "$beam_10k" \
            --argjson rsync_10k "$rsync_10k" \
            '.beam_over_ssh = {
                "1gb": {
                    beam: $beam_1gb,
                    rsync: $rsync_1gb,
                    speedup: (($rsync_1gb.mean / $beam_1gb.mean * 100 | round) / 100)
                },
                "10k": {
                    beam: $beam_10k,
                    rsync: $rsync_10k,
                    speedup: (($rsync_10k.mean / $beam_10k.mean * 100 | round) / 100)
                }
            }')
    fi

    echo "$result"
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    check_deps

    # Resolve beam to absolute path
    BEAM=$(realpath "$BEAM")

    # Set up data directory
    if [[ -z "$DATA_DIR" ]]; then
        AUTO_TMPDIR=$(mktemp -d "${TMPDIR:-/tmp}/beam-bench.XXXXXX")
        DATA_DIR="$AUTO_TMPDIR"
    fi
    mkdir -p "$DATA_DIR"

    local src="$DATA_DIR/src"
    local dst="$DATA_DIR/dst"
    local json_dir="$DATA_DIR/json"
    mkdir -p "$src" "$dst" "$json_dir"

    create_test_data "$src"

    # ── Local benchmarks ──────────────────────────────────────────────────

    if [[ "$SKIP_LOCAL" != "true" ]]; then
        echo "" >&2
        echo "=== Local transfer benchmarks ===" >&2

        run_bench "local-1gb" "$json_dir/local-1gb.json" \
            "rm -rf $dst/*" \
            -n "beam" "$BEAM $src/big.bin $dst/" \
            -n "rsync" "rsync $src/big.bin $dst/"

        run_bench "local-10k" "$json_dir/local-10k.json" \
            "rm -rf $dst/*" \
            -n "beam" "$BEAM -r $src/smallfiles/ $dst/" \
            -n "rsync" "rsync -a $src/smallfiles/ $dst/"
    fi

    # ── Remote benchmarks (beam:// + SSH) ─────────────────────────────────

    if [[ "$SKIP_REMOTE" != "true" ]]; then
        echo "" >&2
        echo "=== Beam protocol benchmarks ===" >&2

        local beam_dst="$DATA_DIR/beam-dst"
        mkdir -p "$beam_dst"

        start_daemon "$beam_dst"
        local beam_url="beam://${DAEMON_TOKEN}@localhost:${DAEMON_PORT}"

        local rsync_ssh_dst="$DATA_DIR/rsync-ssh-dst"
        mkdir -p "$rsync_ssh_dst"

        run_bench "beam-1gb" "$json_dir/beam-1gb.json" \
            "rm -rf $beam_dst/*; rm -rf $rsync_ssh_dst/*" \
            -n "beam" "$BEAM $src/big.bin ${beam_url}/" \
            -n "rsync" "rsync -e ssh $src/big.bin localhost:$rsync_ssh_dst/"

        run_bench "beam-10k" "$json_dir/beam-10k.json" \
            "rm -rf $beam_dst/*; rm -rf $rsync_ssh_dst/*" \
            -n "beam" "$BEAM -r $src/smallfiles/ ${beam_url}/" \
            -n "rsync" "rsync -a -e ssh $src/smallfiles/ localhost:$rsync_ssh_dst/"

        stop_daemon

        # ── Beam-over-SSH benchmarks ──────────────────────────────────────

        if ssh -o BatchMode=yes -o ConnectTimeout=2 localhost true 2>/dev/null; then
            echo "" >&2
            echo "=== Beam-over-SSH benchmarks ===" >&2

            local ssh_beam_dst="$DATA_DIR/ssh-beam-dst"
            local ssh_rsync_dst="$DATA_DIR/ssh-rsync-dst"
            mkdir -p "$ssh_beam_dst" "$ssh_rsync_dst"

            run_bench "ssh-1gb" "$json_dir/ssh-1gb.json" \
                "rm -rf $ssh_beam_dst/*; rm -rf $ssh_rsync_dst/*" \
                -n "beam" "$BEAM $src/big.bin localhost:$ssh_beam_dst/" \
                -n "rsync" "rsync -e ssh $src/big.bin localhost:$ssh_rsync_dst/"

            run_bench "ssh-10k" "$json_dir/ssh-10k.json" \
                "rm -rf $ssh_beam_dst/*; rm -rf $ssh_rsync_dst/*" \
                -n "beam" "$BEAM -r $src/smallfiles/ localhost:$ssh_beam_dst/" \
                -n "rsync" "rsync -a -e ssh $src/smallfiles/ localhost:$ssh_rsync_dst/"
        else
            echo "Warning: localhost SSH not available, skipping beam-over-SSH benchmarks" >&2
        fi
    fi

    # ── Aggregate and output ──────────────────────────────────────────────

    echo "" >&2
    echo "=== Aggregating results ===" >&2

    local final_json
    final_json=$(aggregate_results "$json_dir")

    if [[ -n "$OUTPUT" ]]; then
        echo "$final_json" | jq . > "$OUTPUT"
        echo "Results written to $OUTPUT" >&2
    else
        echo "$final_json" | jq .
    fi
}

main
