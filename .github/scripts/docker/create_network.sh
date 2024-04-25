#!/bin/bash

set -e

docker network ls

output=$(docker network ls | grep ' bridge ')

if [ -z "$output" ]; then
    docker network create --driver nat --attachable --subnet 172.40.0.0/16 docker-cluster-iginx
else
    docker network create -d bridge --attachable --subnet 172.40.0.0/16 docker-cluster-iginx
fi