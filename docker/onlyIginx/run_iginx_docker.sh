#!/bin/bash
VERSION=0.6.0-SNAPSHOT

ip=$1
name=$2
port=$3
logdir="$(pwd)/../../logs/docker_logs"
mkdir -p $logdir
docker run --name="${name}" --privileged -dit --net docker-cluster-iginx --ip ${ip} --add-host=host.docker.internal:host-gateway -v ${logdir}:/iginx/logs/ -p ${port}:6888 iginx:${VERSION}