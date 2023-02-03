#!/bin/sh

set -e

sh -c "zookeeper/bin/zkServer.sh stop"

sh -c "sleep 2"

sh -c "rm -rf zookeeper/data"

sh -c "ls zookeeper;pwd"

sh -c "mkdir zookeeper/data"

sh -c "rm -rf zookeeper/logs"

sh -c "ls zookeeper"

sh -c "mkdir zookeeper/logs"
