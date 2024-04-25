#!/bin/sh

set -e

sh -c "wget -nv https://dlcdn.apache.org/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz --no-check-certificate"

sh -c "tar -zxf apache-zookeeper-3.8.4-bin.tar.gz"

sh -c "mv apache-zookeeper-3.8.4-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zoo.cfg zookeeper/conf/zoo.cfg"

sh -c "zookeeper/bin/zkServer.sh start"

sh -c "zookeeper/bin/zkCli.sh ls /"
