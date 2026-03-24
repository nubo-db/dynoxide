#!/bin/bash
# Run Dynalite's test suite against Dynoxide's HTTP server.
#
# Prerequisites:
#   - Dynoxide built: cargo build --release
#   - Dynalite cloned: git clone https://github.com/architect/dynalite tests/external/dynalite
#   - Dynalite deps installed: cd tests/external/dynalite && npm install
#   - DYNALITE_HOST override added to tests/external/dynalite/test/helpers.js
#
# Usage:
#   ./tests/external/run-dynalite-suite.sh [port]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DYNOXIDE_PORT="${1:-4567}"
RESULTS_DIR="$SCRIPT_DIR/results"
DYNALITE_DIR="$SCRIPT_DIR/dynalite"

mkdir -p "$RESULTS_DIR"

# Check prerequisites
if [ ! -f "$PROJECT_DIR/target/release/dynoxide" ]; then
  echo "Error: Dynoxide not built. Run: cargo build --release"
  exit 1
fi

if [ ! -d "$DYNALITE_DIR/node_modules" ]; then
  echo "Error: Dynalite not set up. Run:"
  echo "  git clone https://github.com/architect/dynalite $DYNALITE_DIR"
  echo "  cd $DYNALITE_DIR && npm install"
  exit 1
fi

# Start Dynoxide
echo "Starting Dynoxide on port $DYNOXIDE_PORT..."
"$PROJECT_DIR/target/release/dynoxide" --port "$DYNOXIDE_PORT" &
DYNOXIDE_PID=$!
trap "kill $DYNOXIDE_PID 2>/dev/null" EXIT

# Wait for server to be ready
for i in $(seq 1 30); do
  nc -z 127.0.0.1 "$DYNOXIDE_PORT" 2>/dev/null && break
  sleep 0.5
done

if ! nc -z 127.0.0.1 "$DYNOXIDE_PORT" 2>/dev/null; then
  echo "Error: Dynoxide failed to start on port $DYNOXIDE_PORT"
  exit 1
fi
echo "Dynoxide ready."

# Run tests
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_FILE="$RESULTS_DIR/dynalite-$TIMESTAMP.txt"

echo "Running Dynalite test suite..."
cd "$DYNALITE_DIR"
REMOTE=1 DYNALITE_HOST="http://127.0.0.1:$DYNOXIDE_PORT" \
  npx mocha --require should --reporter spec -t 10s 2>&1 | tee "$RESULTS_FILE"

echo ""
echo "=== Results saved to: $RESULTS_FILE ==="
grep -E "passing|failing" "$RESULTS_FILE" | tail -2
