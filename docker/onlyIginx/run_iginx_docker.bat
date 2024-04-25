@echo off
set ip=%1
set name=%2
set port=%3
mkdir -p logs/docker_logs
docker run --name="%name%" --privileged -dit --net docker-cluster-iginx --ip %ip% --add-host=host.docker.internal:host-gateway -v logs/docker_logs/:/iginx/logs/ -p %port%:6888 iginx:0.6.0