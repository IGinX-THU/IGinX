#!/bin/bash

# 启用错误检测
set -e

# 函数：记录时间戳日志
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# 检查必要参数
if [ -z "$1" ]; then
  log "ERROR: Port parameter is required"
  exit 1
fi

port=$1
log "Starting Dameng database on port $port"

# 检查容器是否存在
if docker ps -a | grep -q "dm8-$port"; then
  log "Container dm8-$port already exists, restarting it"
  
  docker restart "dm8-$port" || {
    log "ERROR: Failed to restart container"
    exit 1
  }
  log "Container restarted successfully"

  restart_time=$(date '+%Y-%m-%dT%H:%M:%S')
  log "restart_time: $restart_time"
else
  # 创建新容器
  log "Creating new container dm8-$port"
  docker run -d -p $port:5236 --restart=always \
    --name=dm8-$port \
    --privileged=true \
    -e LD_LIBRARY_PATH=/opt/dmdbms/bin \
    -e PAGE_SIZE=16 \
    -e EXTENT_SIZE=32 \
    -e LOG_SIZE=1024 \
    -e UNICODE_FLAG=1 \
    -e INSTANCE_NAME=dm8_test \
    -v /mnt/c/opt/data_$port:/opt/dmdbms/data \
    dm8_single:dm8_20241022_rev244896_x86_rh6_64 || {
      log "ERROR: Failed to start container"
      exit 1
    }
  log "Container created successfully"

  restart_time=$(date '+%Y-%m-%dT%H:%M:%S')
    log "restart_time: $restart_time"
fi

# 检查容器是否运行
if ! docker ps | grep -q "dm8-$port"; then
  log "ERROR: Container is not running after startup/restart"
  docker logs "dm8-$port" || true
  exit 1
fi

log "Container is running, checking database status"

# 动态等待数据库启动，最多等待2分钟
total_wait=120
elapsed=0
check_interval=10  # 每10秒检查一次，更合理的频率
database_started=false

log "Waiting for database to start (timeout: ${total_wait}s)..."

while [ $elapsed -lt $total_wait ]; do
  # 检查最近的日志（只考虑重启后的日志）
  recent_logs=$(docker logs --since $restart_time "dm8-$port" 2>&1)

  if echo "$recent_logs" | grep -q "DM Database is OK"; then
    database_started=true
    log "Found 'DM Database is OK' in logs after ${elapsed} seconds"
    break
  fi
  
  echo "'DM Database is OK' not found in the logs, retrying in 10 seconds..."
  sleep $check_interval
  elapsed=$((elapsed + check_interval))
done

# 检查数据库是否成功启动
if [ "$database_started" = true ]; then
  log "SUCCESS: Dameng database started successfully on port $port"
  docker ps | grep "dm8-$port"
  exit 0
else
  log "ERROR: Database failed to start within ${total_wait} seconds"
  log "Last 10 lines of recent logs:"
  docker logs --since "$restart_time" --tail 10 "dm8-$port" || true
  exit 1
fi