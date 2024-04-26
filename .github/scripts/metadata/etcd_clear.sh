#!/bin/sh

set -e

sh -c "ls go/bin"

sh -c "ls ."

os=$(uname -s)

kill_process_on_port() {
    port=$1
    if [ -n "$MSYSTEM" ]; then
        # win
        pid=$(netstat -ano | findstr :$port | awk '{print $5}' | uniq)
        if [ ! -z "$pid" ]; then
            echo "Killing etcd process $pid on port $port"
            sh -c "taskkill -f -pid $pid"
        else
            echo "No process found listening on port $port"
        fi
    else
        # linux mac
        pid=$(lsof -ti:$port)
        if [ ! -z "$pid" ]; then
            echo "Killing etcd process $pid on port $port"
            kill -9 $pid
        else
            echo "No process found listening on port $port"
        fi
    fi
}

kill_process_on_port 2379

sh -c "sleep 2"

sh -c "rm -rf go/bin/*.etcd"

sh -c "rm -rf *.etcd"