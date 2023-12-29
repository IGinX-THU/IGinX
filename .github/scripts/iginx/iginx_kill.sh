#!/bin/bash

# below JavaApp is the name of running Java process
jps

pids=( $(jps | grep Iginx | awk '{print $1}') )

if [ -n "$MSYSTEM" ]; then
  # need to use taskkill on windows
  for pid in "${pids[@]}"; do
       echo "killing $pid"
       sh -c "taskkill -f -pid $pid"
  done
else
  for pid in "${pids[@]}"; do
       echo "killing $pid"
       kill -9 $pid
  done
fi