#!/bin/bash

port=$1
name="oceanbase-ce-$port"

wsl docker restart "$name"
sleep 5
