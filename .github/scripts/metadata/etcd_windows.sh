#!/bin/sh

set -e

sh -c "curl -LJO https://go.dev/dl/go1.21.5.windows-amd64.zip -o go1.21.5.windows-amd64.zip"

sh -c "unzip -qq go1.21.5.windows-amd64.zip"

sh -c "ls ./"

sh -c "ls go"

export GOROOT=$PWD

export GOPATH=$GOROOT/go

export GOBIN=$GOPATH/bin

export PATH=$PATH:$GOBIN

sh -c "curl -LJO https://github.com/etcd-io/etcd/releases/download/v3.4.28/etcd-v3.4.28-windows-amd64.zip -o etcd-v3.4.28-windows-amd64.zip"

sh -c "unzip -qq etcd-v3.4.28-windows-amd64.zip"

sh -c "mv etcd-v3.4.28-windows-amd64/etcd* $GOBIN"

powershell -command "Start-Process etcd -NoNewWindow -RedirectStandardOutput 'etcd-run.log' -RedirectStandardError 'etcd-error.log'"
