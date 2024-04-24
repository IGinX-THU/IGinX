@echo off
set ip=%1
set name=%2
set port=%3
docker run --name="%name%" --privileged -dit --net docker-cluster-iginx --ip %ip% -p %port%:6888 iginx:0.6.0