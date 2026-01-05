#!/bin/bash

port=$1
name="oceanbase-ce-$port"

docker stop "$name"
sleep 5
