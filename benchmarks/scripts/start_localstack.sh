#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="dynoxide-bench-ls"
PORT="${1:-4566}"
TIMEOUT=60

echo "Starting LocalStack on port $PORT..."

# Remove any existing container
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

# Start LocalStack with DynamoDB service only
START_TIME=$(python3 -c 'import time; print(time.time())')
docker run --rm -d \
    -p "$PORT:4566" \
    --name "$CONTAINER_NAME" \
    -e SERVICES=dynamodb \
    localstack/localstack:latest

# Wait until health check passes and DynamoDB is running
echo -n "Waiting for LocalStack to be ready"
for i in $(seq 1 "$TIMEOUT"); do
    HEALTH=$(curl -s "http://localhost:$PORT/_localstack/health" 2>/dev/null || echo "{}")
    if echo "$HEALTH" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    services = data.get('services', {})
    if services.get('dynamodb') in ('running', 'available'):
        sys.exit(0)
except:
    pass
sys.exit(1)
" 2>/dev/null; then
        END_TIME=$(python3 -c 'import time; print(time.time())')
        STARTUP_MS=$(python3 -c "print(int(($END_TIME - $START_TIME) * 1000))")
        echo ""
        echo "LocalStack ready in ${STARTUP_MS}ms"
        echo "Endpoint: http://localhost:$PORT"
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo ""
echo "ERROR: LocalStack failed to start within ${TIMEOUT}s"
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
exit 1
