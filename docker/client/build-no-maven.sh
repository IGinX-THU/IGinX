#!/bin/bash
VERSION=0.6.0-SNAPSHOT

docker build --build-arg VERSION=${VERSION} --file Dockerfile-no-maven -t iginx-client:${VERSION} ../../client