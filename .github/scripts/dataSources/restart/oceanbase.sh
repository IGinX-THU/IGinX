#!/bin/bash

port=$1
name="oceanbase-ce-$port"

docker restart "$name"
# OceanBase boot may take several minutes
sleep 5
