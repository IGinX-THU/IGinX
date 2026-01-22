#!/bin/bash

port=$1
name="oceanbase-ce-$port"

echo "Restarting ${name}..."
docker restart "$name"

# Wait a bit for container to start
sleep 10

# Wait for OceanBase to be fully ready
echo "Waiting for ${name} to be fully ready..."
boot_detected=false
restart_attempts=0
max_restart_attempts=2

for i in $(seq 1 90); do
  echo "Attempt $i/90: Checking ${name} status..."
  
  # Check if container is running
  if ! docker ps --filter "name=${name}" --filter "status=running" --format '{{.Names}}' | grep -q "^${name}$"; then
    echo "Container ${name} is not running!"
    container_status=$(docker ps -a --filter "name=${name}" --format '{{.Status}}')
    echo "Container status: ${container_status}"
    
    # Check logs for critical errors
    logs=$(docker logs --tail 200 "${name}" 2>&1)
    
    # Check for memory issues
    if echo "$logs" | grep -q "not enough memory\|OBD-2000"; then
      echo "ERROR: OceanBase requires more memory than available!"
      echo "Last 50 lines of logs:"
      echo "$logs" | tail -n 50
      exit 1
    fi
    
    # Check for boot failure
    if echo "$logs" | grep -q "boot failed\|Failed to start.*observer\|OBD-2002"; then
      echo "ERROR: OceanBase failed to start!"
      echo "Last 50 lines of logs:"
      echo "$logs" | tail -n 50
      
      # Try restarting once more if we haven't exceeded max attempts
      if [ $restart_attempts -lt $max_restart_attempts ]; then
        restart_attempts=$((restart_attempts + 1))
        echo "Attempting to restart container (attempt $restart_attempts/$max_restart_attempts)..."
        docker restart "$name"
        sleep 10
        continue
      else
        echo "Max restart attempts reached. Giving up."
        exit 1
      fi
    fi
    
    # If container exited for unknown reason, try restarting once
    if [ $restart_attempts -lt $max_restart_attempts ]; then
      restart_attempts=$((restart_attempts + 1))
      echo "Container exited unexpectedly. Attempting restart (attempt $restart_attempts/$max_restart_attempts)..."
      docker restart "$name"
      sleep 10
      continue
    else
      echo "Container keeps exiting. Last 50 lines of logs:"
      echo "$logs" | tail -n 50
      exit 1
    fi
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

echo "ERROR: ${name} failed to restart properly after 450 seconds"
echo "Container status:"
docker ps -a --filter "name=${name}"
echo "Last 100 lines of logs:"
docker logs --tail 100 "${name}" 2>&1
exit 1
