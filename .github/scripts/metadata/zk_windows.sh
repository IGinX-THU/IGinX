#!/bin/sh

set -e

sh -c "curl -LJO https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz -o apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "tar -zxf apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "mv apache-zookeeper-3.7.2-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zooWin.cfg zookeeper/conf/zoo.cfg"

cd zookeeper

sh -c "mkdir logs"

sh -c "mkdir data"

powershell -Command "Start-Process -FilePath 'bin/zkServer.cmd' -NoNewWindow -RedirectStandardOutput 'logs/zookeeper.log' -RedirectStandardError 'logs/zookeeper-error.log'"

sleep 3

echo $(netstat -ano | grep 2181)
