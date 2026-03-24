#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="dynoxide-bench-ddb"
PORT="${1:-8000}"
TIMEOUT=30

echo "Starting DynamoDB Local on port $PORT..."

# Remove any existing container
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

# Start DynamoDB Local
START_TIME=$(python3 -c 'import time; print(time.time())')
docker run --rm -d \
    -p "$PORT:8000" \
    --name "$CONTAINER_NAME" \
    amazon/dynamodb-local:latest \
    -jar DynamoDBLocal.jar -inMemory -sharedDb

# Wait until responsive (use curl — DynamoDB Local rejects --no-sign-request)
echo -n "Waiting for DynamoDB Local to be ready"
for i in $(seq 1 "$TIMEOUT"); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/x-amz-json-1.0" \
        -H "X-Amz-Target: DynamoDB_20120810.ListTables" \
        -d "{}" \
        "http://localhost:$PORT" 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" -gt 0 ] 2>/dev/null && [ "$HTTP_CODE" -lt 500 ] 2>/dev/null; then
        END_TIME=$(python3 -c 'import time; print(time.time())')
        STARTUP_MS=$(python3 -c "print(int(($END_TIME - $START_TIME) * 1000))")
        echo ""
        echo "DynamoDB Local ready in ${STARTUP_MS}ms"
        echo "Endpoint: http://localhost:$PORT"
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo ""
echo "ERROR: DynamoDB Local failed to start within ${TIMEOUT}s"
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
exit 1
