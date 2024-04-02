#!/bin/sh

set -e

sh -c "ls go/bin"

sh -c "ls ."

os=$(uname -s)

kill_process_on_port() {
    port=$1
    case $os in
        Linux|Darwin)
            pid=$(lsof -ti:$port)
            if [ ! -z "$pid" ]; then
                echo "Killing etcd process $pid on port $port"
                kill -9 $pid
            else
                echo "No process found listening on port $port"
            fi
            ;;
        CYGWIN*|MINGW32*|MSYS*|Windows_NT)
            pid=$(netstat -ano | findstr :$port | awk '{print $5}' | uniq)
            if [ ! -z "$pid" ]; then
                echo "Killing etcd process $pid on port $port"
                Taskkill /PID $pid /F
            else
                echo "No process found listening on port $port"
            fi
            ;;
        *)
            echo "Unsupported OS: $os"
            ;;
    esac
}

kill_process_on_port 2379

sh -c "sleep 2"

sh -c "rm -rf go/bin/*.etcd"

sh -c "rm -rf *.etcd"