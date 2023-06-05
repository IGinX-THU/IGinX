#!/bin/sh

set -e

sh -c "zookeeper/bin/zkCli.sh deleteall /iginx; zookeeper/bin/zkCli.sh deleteall /fragment; zookeeper/bin/zkCli.sh deleteall /lock; zookeeper/bin/zkCli.sh deleteall /storage; zookeeper/bin/zkCli.sh deleteall /unit; zookeeper/bin/zkCli.sh deleteall /statistics; zookeeper/bin/zkCli.sh deleteall /notification; zookeeper/bin/zkCli.sh deleteall /user; zookeeper/bin/zkCli.sh deleteall /counter; zookeeper/bin/zkCli.sh deleteall /schema; zookeeper/bin/zkCli.sh deleteall /policy; zookeeper/bin/zkCli.sh deleteall /timeseries; zookeeper/bin/zkCli.sh deleteall /transform"

sh -c "zookeeper/bin/zkServer.sh stop"

sh -c "sleep 2"

sh -c "rm -rf zookeeper/data"

sh -c "mkdir zookeeper/data"

sh -c "rm -rf zookeeper/logs"

sh -c "mkdir zookeeper/logs"
