#!/bin/bash

port=$1
name="oceanbase-ce-$port"

docker restart "$name"
sleep 5
