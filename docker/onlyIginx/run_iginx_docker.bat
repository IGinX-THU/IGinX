@echo off
set ip=%1
set port=%2
docker run --name="iginx0" --privileged -dit --net docker-cluster-iginx --ip %ip% -p %port%:6888 iginx:0.6.0