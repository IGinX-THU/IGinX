@echo off
set "current_dir=%CD%"
@REM 将路径中的单反斜线替换为双反斜线
set "double_slash_dir=%current_dir:\=\\%"

@REM 初始化变量
set "name=iginx-client"
set "logdir=%double_slash_dir%\\..\\..\\logs\\docker_logs"

set ip=%1
set name=%2
set port=%3
mkdir -p logs/docker_logs
docker run --name="%name%" --privileged -dit --net docker-cluster-iginx --ip %ip% --add-host=host.docker.internal:host-gateway -v %logdir%:/iginx/logs/ -p %port%:6888 iginx:0.7.0-SNAPSHOT