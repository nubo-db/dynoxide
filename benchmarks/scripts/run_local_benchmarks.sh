#!/usr/bin/env bash
set -euo pipefail

# Run all benchmarks locally on Apple Silicon and update local_ markers
# in both READMEs.
#
# Prerequisites:
#   - Rust toolchain (cargo)
#   - Docker (for DynamoDB Local and LocalStack comparisons)
#   - Python 3 (for criterion output parsing and README update)
#
# Usage:
#   benchmarks/scripts/run_local_benchmarks.sh              # run and update
#   benchmarks/scripts/run_local_benchmarks.sh --dry-run    # run but don't write
#   benchmarks/scripts/run_local_benchmarks.sh --skip-docker # skip Docker-dependent benchmarks

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$REPO_ROOT/benchmarks/local-results"

DRY_RUN=""
SKIP_DOCKER=false

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN="--dry-run" ;;
        --skip-docker) SKIP_DOCKER=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

echo "=== Local Benchmark Suite ==="
echo "Machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
echo "RAM: $(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.0f GB", $1/1073741824}')"
echo ""

rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

echo "--- Building Dynoxide (release) ---"
cd "$REPO_ROOT"
cargo build --release --bin dynoxide 2>&1 | tail -1

echo "--- Building benchmark binaries (release) ---"
cd "$REPO_ROOT/benchmarks"
cargo build --release \
    --bin workload_driver \
    --bin ci_pipeline_bench \
    --bin startup_bench \
    --bin memory_profiler 2>&1 | tail -1

# ---------------------------------------------------------------------------
# Docker services
# ---------------------------------------------------------------------------

DDB_LOCAL_PORT=8000
DDB_LOCAL_CONTAINER=""
DYNOXIDE_PID=""

cleanup() {
    echo ""
    echo "--- Cleanup ---"
    [ -n "$DYNOXIDE_PID" ] && kill "$DYNOXIDE_PID" 2>/dev/null && echo "Stopped Dynoxide" || true
    [ -n "$DDB_LOCAL_CONTAINER" ] && docker rm -f "$DDB_LOCAL_CONTAINER" >/dev/null 2>&1 && echo "Removed DynamoDB Local" || true
    # Note: LocalStack containers are managed by startup_bench/memory_profiler directly
}
trap cleanup EXIT

if [ "$SKIP_DOCKER" = false ] && command -v docker &>/dev/null; then
    echo ""
    echo "--- Starting DynamoDB Local ---"
    DDB_LOCAL_CONTAINER=$(docker run -d --rm -p ${DDB_LOCAL_PORT}:8000 amazon/dynamodb-local:latest)
    echo "Container: ${DDB_LOCAL_CONTAINER:0:12}"

    # Wait for DDB Local
    for i in $(seq 1 30); do
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST "http://localhost:${DDB_LOCAL_PORT}" \
            -H 'Content-Type: application/x-amz-json-1.0' \
            -H 'X-Amz-Target: DynamoDB_20120810.ListTables' \
            -d '{}' 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" -gt 0 ] 2>/dev/null && [ "$HTTP_CODE" -lt 500 ] 2>/dev/null; then
            echo "DynamoDB Local ready (HTTP $HTTP_CODE)"
            break
        fi
        [ "$i" -eq 30 ] && echo "WARNING: DynamoDB Local failed to start" >&2
        sleep 1
    done

    # Note: LocalStack is NOT started here. The startup_bench and memory_profiler
    # manage their own LocalStack containers internally (they measure cold start).
    # A long-running container here would conflict on port 4566.
else
    if [ "$SKIP_DOCKER" = true ]; then
        echo ""
        echo "--- Skipping Docker benchmarks (--skip-docker) ---"
    else
        echo ""
        echo "WARNING: Docker not available, skipping DDB Local / LocalStack benchmarks" >&2
    fi
fi

# ---------------------------------------------------------------------------
# Start Dynoxide HTTP server
# ---------------------------------------------------------------------------

echo ""
echo "--- Starting Dynoxide HTTP server ---"
DYNOXIDE_PORT=8123
"$REPO_ROOT/target/release/dynoxide" --port "$DYNOXIDE_PORT" &
DYNOXIDE_PID=$!

for i in $(seq 1 10); do
    if curl -sf -X POST "http://localhost:${DYNOXIDE_PORT}" \
        -H 'X-Amz-Target: DynamoDB_20120810.ListTables' \
        -H 'Content-Type: application/x-amz-json-1.0' \
        -d '{}' > /dev/null 2>&1; then
        echo "Dynoxide HTTP ready on port $DYNOXIDE_PORT"
        break
    fi
    sleep 0.5
done

# ---------------------------------------------------------------------------
# Run benchmarks
# ---------------------------------------------------------------------------

echo ""
echo "--- Startup benchmark (10 reps) ---"
cd "$REPO_ROOT/benchmarks"
cargo run --release --bin startup_bench -- \
    --reps 10 \
    --output "$RESULTS_DIR/startup.json"
echo "Done"

echo ""
echo "--- Workload: Dynoxide HTTP ---"
cargo run --release --bin workload_driver -- \
    --endpoint-url "http://localhost:${DYNOXIDE_PORT}" \
    --output "$RESULTS_DIR/dynoxide_http.json"
echo "Done"

if [ -n "$DDB_LOCAL_CONTAINER" ]; then
    echo ""
    echo "--- Workload: DynamoDB Local ---"
    cargo run --release --bin workload_driver -- \
        --endpoint-url "http://localhost:${DDB_LOCAL_PORT}" \
        --output "$RESULTS_DIR/dynamodb_local.json"
    echo "Done"
fi

echo ""
echo "--- CI pipeline benchmark (50 tests) ---"
CI_ARGS=()
if [ -n "$DDB_LOCAL_CONTAINER" ]; then
    CI_ARGS+=(--ddb-endpoint "http://localhost:${DDB_LOCAL_PORT}")
fi
cargo run --release --bin ci_pipeline_bench -- \
    "${CI_ARGS[@]}" \
    --output "$RESULTS_DIR/ci_pipeline.json"
echo "Done"

echo ""
echo "--- Memory profiler ---"
cargo run --release --bin memory_profiler > "$RESULTS_DIR/memory_profile.csv"
echo "Done"

echo ""
echo "--- Criterion micro-benchmarks (embedded_micro) ---"
cargo bench --bench embedded_micro -- --output-format bencher | tee "$RESULTS_DIR/criterion_output.txt"
python3 -c "
import json, re
results = {}
for line in open('$RESULTS_DIR/criterion_output.txt'):
    m = re.match(r'^test\s+(.+?)\s+\.\.\.\s+bench:\s+([\d,]+)\s+ns/iter', line)
    if m:
        results[m.group(1)] = int(m.group(2).replace(',', ''))
with open('$RESULTS_DIR/criterion_baseline.json', 'w') as f:
    json.dump(results, f, indent=2)
print(f'Parsed {len(results)} criterion benchmarks')
"

# ---------------------------------------------------------------------------
# Extract CI ratios
# ---------------------------------------------------------------------------

echo ""
echo "--- Extracting CI ratios ---"
python3 -c "
import json
with open('$RESULTS_DIR/ci_pipeline.json') as f:
    data = json.load(f)
ratios = {}
for row in data.get('summary', []):
    speedup = row.get('speedup_vs_ddb_local')
    if speedup is not None:
        key = row['mode'] + '_' + row['execution']
        ratios[key] = round(speedup, 1)
with open('$RESULTS_DIR/ci_ratios.json', 'w') as f:
    json.dump(ratios, f, indent=2)
print(f'Extracted {len(ratios)} ratios')
"

# ---------------------------------------------------------------------------
# Collect system info
# ---------------------------------------------------------------------------

echo ""
echo "--- Collecting system info ---"
bash "$SCRIPT_DIR/collect_system_info.sh" > "$RESULTS_DIR/system_info.json"

# ---------------------------------------------------------------------------
# Consolidate into run_summary.json
# ---------------------------------------------------------------------------

echo ""
echo "--- Consolidating results ---"
python3 "$SCRIPT_DIR/append_history.py" \
    --results-dir "$RESULTS_DIR" \
    --output "$RESULTS_DIR/run_summary.json"

# ---------------------------------------------------------------------------
# Update READMEs
# ---------------------------------------------------------------------------

echo ""
echo "--- Updating local_ markers in READMEs ---"
python3 "$SCRIPT_DIR/update_readme_benchmarks.py" \
    --data-dir "$RESULTS_DIR" \
    --readme "$REPO_ROOT/README.md" \
    --benchmarks-readme "$REPO_ROOT/benchmarks/README.md" \
    --local \
    $DRY_RUN

echo ""
echo "=== Done ==="
echo "Results saved to: $RESULTS_DIR"
echo "Run 'git diff README.md benchmarks/README.md' to review changes"
