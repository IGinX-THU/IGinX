#!/bin/bash

set -e

CONTAINER_NAME="$1"

echo "=================================================="
echo "📄 Showing docker logs for container: $CONTAINER_NAME"
docker logs "$CONTAINER_NAME" --tail 100 || true

echo "=================================================="
echo "📄 Checking ORACLE_BASE inside container..."
ORACLE_BASE=$(docker exec "$CONTAINER_NAME" bash -c 'echo $ORACLE_BASE')

if [ -z "$ORACLE_BASE" ]; then
  echo "❌ Failed to retrieve ORACLE_BASE"
  exit 1
fi

echo "✅ ORACLE_BASE is $ORACLE_BASE"

echo "=================================================="
echo "📄 Reading Oracle alert log..."
INSTANCE=$(docker exec oracle23-1521 bash -c "ps -ef | grep pmon | grep -v grep | awk -F_ '{print \$NF}'")
ALERT_LOG_PATH="/opt/oracle/diag/rdbms/$INSTANCE/$INSTANCE/trace/alert_${INSTANCE}.log"

docker exec "$CONTAINER_NAME" bash -c "if [ -f '$ALERT_LOG_PATH' ]; then tail -100 '$ALERT_LOG_PATH'; else echo '❌ Alert log not found at $ALERT_LOG_PATH'; fi"

echo "=================================================="
echo "📄 Reading Listener log..."
LISTENER_DIR="$ORACLE_BASE/diag/tnslsnr/$(docker exec "$CONTAINER_NAME" hostname)/listener/trace"
LISTENER_LOG_PATH="$LISTENER_DIR/listener.log"

docker exec "$CONTAINER_NAME" bash -c "if [ -f '$LISTENER_LOG_PATH' ]; then tail -100 '$LISTENER_LOG_PATH'; else echo '❌ Listener log not found at $LISTENER_LOG_PATH'; fi"
