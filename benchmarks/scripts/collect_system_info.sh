#!/usr/bin/env bash
set -euo pipefail

# Collect system information for benchmark reproducibility.
# Outputs JSON to stdout.
# Shell variables are passed as arguments to Python (not interpolated into strings)
# to avoid injection from values containing quotes or special characters.

OS=$(uname -s)
KERNEL=$(uname -r)
ARCH=$(uname -m)

# CPU info
if [[ "$OS" == "Darwin" ]]; then
    CPU_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
    CPU_CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo "unknown")
    TOTAL_RAM_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo "0")
    TOTAL_RAM_GB=$(python3 -c "import sys; print(round(int(sys.argv[1]) / (1024**3), 1))" "$TOTAL_RAM_BYTES")
elif [[ "$OS" == "Linux" ]]; then
    CPU_MODEL=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | xargs || echo "unknown")
    CPU_CORES=$(nproc 2>/dev/null || echo "unknown")
    TOTAL_RAM_KB=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo "0")
    TOTAL_RAM_GB=$(python3 -c "import sys; print(round(int(sys.argv[1]) / (1024**2), 1))" "$TOTAL_RAM_KB")
else
    CPU_MODEL="unknown"
    CPU_CORES="unknown"
    TOTAL_RAM_GB="unknown"
fi

# Dynoxide version
DYNOXIDE_VERSION=$(cargo metadata --format-version=1 --no-deps 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for pkg in data.get('packages', []):
    if pkg['name'] == 'dynoxide-rs':
        print(pkg['version'])
        break
" 2>/dev/null || echo "unknown")

DYNOXIDE_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# DynamoDB Local version (if Docker available)
DDB_LOCAL_TAG="not available"
if command -v docker &>/dev/null; then
    DDB_LOCAL_TAG=$(docker inspect amazon/dynamodb-local:latest --format '{{.Id}}' 2>/dev/null | head -c 12 || echo "not available")
fi

# Dynoxide binary size (if built)
DYNOXIDE_BINARY_BYTES=$(stat --format=%s target/release/dynoxide 2>/dev/null || stat -f%z target/release/dynoxide 2>/dev/null || echo "null")

# Docker image sizes (if Docker available)
DDB_LOCAL_IMAGE_BYTES="null"
LOCALSTACK_IMAGE_BYTES="null"
if command -v docker &>/dev/null; then
    DDB_LOCAL_IMAGE_BYTES=$(docker image inspect amazon/dynamodb-local:latest --format='{{.Size}}' 2>/dev/null || echo "null")
    LOCALSTACK_IMAGE_BYTES=$(docker image inspect localstack/localstack:latest --format='{{.Size}}' 2>/dev/null || echo "null")
fi

# Rust version
RUST_VERSION=$(rustc --version 2>/dev/null || echo "unknown")

# Output JSON — pass all values as argv to avoid shell injection
python3 -c "
import json, sys, datetime
info = {
    'os': sys.argv[1],
    'kernel': sys.argv[2],
    'arch': sys.argv[3],
    'cpu_model': sys.argv[4],
    'cpu_cores': sys.argv[5],
    'total_ram_gb': sys.argv[6],
    'rust_version': sys.argv[7],
    'dynoxide_version': sys.argv[8],
    'dynoxide_commit': sys.argv[9],
    'dynamodb_local_image_id': sys.argv[10],
    'dynoxide_binary_bytes': None if sys.argv[11] == 'null' else int(sys.argv[11]),
    'dynamodb_local_image_bytes': None if sys.argv[12] == 'null' else int(sys.argv[12]),
    'localstack_image_bytes': None if sys.argv[13] == 'null' else int(sys.argv[13]),
    'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
}
json.dump(info, sys.stdout, indent=2)
print()
" "$OS" "$KERNEL" "$ARCH" "$CPU_MODEL" "$CPU_CORES" "$TOTAL_RAM_GB" \
  "$RUST_VERSION" "$DYNOXIDE_VERSION" "$DYNOXIDE_COMMIT" "$DDB_LOCAL_TAG" \
  "$DYNOXIDE_BINARY_BYTES" "$DDB_LOCAL_IMAGE_BYTES" "$LOCALSTACK_IMAGE_BYTES"
