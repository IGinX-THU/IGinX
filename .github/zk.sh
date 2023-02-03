#!/bin/sh

set -e

sh -c "wget -nv https://dlcdn.apache.org/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz --no-check-certificate"

sh -c "tar -xzf apache-zookeeper-3.7.1-bin.tar.gz"

sh -c "mv apache-zookeeper-3.7.1-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zoo.cfg zookeeper/conf/zoo.cfg"

sh -c "zookeeper/bin/zkServer.sh start"

sh -c "zookeeper/bin/zkCli.sh ls /"
