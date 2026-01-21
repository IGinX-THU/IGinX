#!/bin/bash

port=$1
name="oceanbase-ce-$port"

docker restart "$name"

# Wait for OceanBase to be fully ready
echo "Waiting for ${name} to be fully ready..."
for i in $(seq 1 90); do
  if docker logs "${name}" 2>&1 | tail -n 20 | grep -q "boot success"; then
    echo "${name} boot success detected in logs"
    # Additionally verify MySQL port is accepting connections
    if docker exec "${name}" mysql -uroot -h127.0.0.1 -P2881 -e "SELECT 1" >/dev/null 2>&1; then
      echo "${name} MySQL port is ready"
      exit 0
    fi
  fi
  sleep 5
done

echo "Warning: ${name} may not be fully ready after restart"
exit 1
