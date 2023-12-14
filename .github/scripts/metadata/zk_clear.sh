#!/bin/bash

set -e

if [ -n "$MSYSTEM" ]; then
  PORT=2181

#  PID=$(netstat -ano | grep $PORT | awk '{print $5}' | uniq)

  readarray -t results < <(netstat -ano | grep $PORT | awk '{print $5}' | uniq)

  for PID in "${results[@]}"; do
    if [ -z "$PID" ]; then
        echo "Can't find zookeeper process $PID"
    else
        sh -c "taskkill -f -pid $PID"
    fi
  done
else
  sh -c "zookeeper/bin/zkServer.sh stop"
fi

sh -c "sleep 2"

sh -c "rm -rf zookeeper/data"

sh -c "mkdir zookeeper/data"

sh -c "rm -rf zookeeper/logs"

sh -c "mkdir zookeeper/logs"
