@REM
@REM IGinX - the polystore system with high performance
@REM Copyright (C) Tsinghua University
@REM
@REM This program is free software: you can redistribute it and/or modify
@REM it under the terms of the GNU General Public License as published by
@REM the Free Software Foundation, either version 3 of the License, or
@REM (at your option) any later version.
@REM
@REM This program is distributed in the hope that it will be useful,
@REM but WITHOUT ANY WARRANTY; without even the implied warranty of
@REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
@REM GNU General Public License for more details.
@REM
@REM You should have received a copy of the GNU General Public License
@REM along with this program. If not, see <http://www.gnu.org/licenses/>.
@REM

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