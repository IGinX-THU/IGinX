@echo off
set name=%1
set port=%2
docker run --name="%name%" --privileged -dit -p %port%:6888 iginx:0.6.0