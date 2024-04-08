#!/bin/bash

echo "Kill IGinX"
jps | grep -w 'Iginx'| awk '{print $1}' | xargs -r kill -9

echo "Kill ZooKeeper"
jps | grep -w 'QuorumPeerMain' | awk '{print $1}' | xargs -r kill -9

