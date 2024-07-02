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