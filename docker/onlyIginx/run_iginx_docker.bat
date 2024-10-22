@REM
@REM IGinX - the polystore system with high performance
@REM Copyright (C) Tsinghua University
@REM TSIGinX@gmail.com
@REM
@REM This program is free software; you can redistribute it and/or
@REM modify it under the terms of the GNU Lesser General Public
@REM License as published by the Free Software Foundation; either
@REM version 3 of the License, or (at your option) any later version.
@REM
@REM This program is distributed in the hope that it will be useful,
@REM but WITHOUT ANY WARRANTY; without even the implied warranty of
@REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
@REM Lesser General Public License for more details.
@REM
@REM You should have received a copy of the GNU Lesser General Public License
@REM along with this program; if not, write to the Free Software Foundation,
@REM Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
docker run --name="%name%" --privileged -dit --net docker-cluster-iginx --ip %ip% --add-host=host.docker.internal:host-gateway -v %logdir%:/iginx/logs/ -p %port%:6888 iginx:0.8.0-SNAPSHOT