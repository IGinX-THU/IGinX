#!/bin/bash

port=$1
name="oceanbase-ce-$port"

echo "Restarting ${name}..."
docker restart "$name"

# Wait a bit for container to start
sleep 10

# Wait for OceanBase to be fully ready (macOS may need more time)
echo "Waiting for ${name} to be fully ready..."
boot_detected=false
for i in $(seq 1 120); do
  echo "Attempt $i/120: Checking ${name} status..."
  
  # Check if container is running
  if ! docker ps --filter "name=${name}" --filter "status=running" --format '{{.Names}}' | grep -q "^${name}$"; then
    echo "Container ${name} is not running!"
    docker ps -a --filter "name=${name}"
    sleep 5
    continue
  fi
  
  # Check logs for boot success
  if docker logs "${name}" 2>&1 | tail -n 50 | grep -q "boot success"; then
    if [ "$boot_detected" = false ]; then
      echo "${name} boot success detected in logs"
      boot_detected=true
    fi
    
    # Wait a bit after boot before testing connection
    sleep 3
    
    # Verify MySQL port is accepting connections
    if docker exec "${name}" mysql -uroot -h127.0.0.1 -P2881 -e "SELECT 1" >/dev/null 2>&1; then
      echo "${name} MySQL port is ready - restart successful!"
      exit 0
    else
      echo "MySQL connection test failed, waiting..."
    fi
  fi
  sleep 5
done

echo "ERROR: ${name} failed to restart properly after 600 seconds"
echo "Container status:"
docker ps -a --filter "name=${name}"
echo "Last 100 lines of logs:"
docker logs --tail 100 "${name}" 2>&1
exit 1
