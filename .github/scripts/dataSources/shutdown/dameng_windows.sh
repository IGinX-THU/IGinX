#!/bin/bash

port=$1

# 检查容器是否存在
if wsl docker ps -a | grep -q "dm8-$port"; then
  echo "Container dm8-$port is still running, stopping it"
  wsl docker stop "dm8-$port" || {
    echo "ERROR: Failed to stop container"
    exit 1
  }
  echo "Container stopped successfully"
else
  echo "Container dm8-$port is already stopped"
fi