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
setlocal enabledelayedexpansion

set current_dir=%CD%
set double_slash_dir=%current_dir:\=/%

set name=iginx-client
set datadir=%double_slash_dir%/data

:parse_args
if "%~1"=="" goto end_parse_args
if "%~1"=="-n" (
     set name=%~2
     shift
 )
if "%~1"=="--datadir" (
     set datadir=%~2
     shift
 )
if "%~1"=="--ip" (
     set ip=%~2
     shift
 )
if "%~1"=="--net" (
     set net=%~2
     shift
 )
shift
goto parse_args
:end_parse_args


if not exist "%datadir%" (
    mkdir "%datadir%"
)

set command=docker run --name="%name%" -dit --add-host=host.docker.internal:host-gateway --mount type=bind,source=!datadir!,target=C:/iginx_client/data iginx-client:0.7.0-SNAPSHOT
echo %command%
%command%

endlocal