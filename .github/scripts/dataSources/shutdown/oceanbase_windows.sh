#!/bin/bash

port=$1
name="oceanbase-ce-$port"

wsl docker stop "$name"
sleep 5
