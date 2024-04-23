@echo off
setlocal enabledelayedexpansion

set "current_dir=%CD%"
rem 将路径中的单反斜线替换为双反斜线
set "double_slash_dir=%current_dir:\=\\%"

rem 初始化变量
set "name=iginx-client"
set "datadir=%double_slash_dir%\\data"

rem 检查参数
:parse_args
if "%~1"=="" goto end_parse_args
if "%~1"=="-n" (
     set "name=%~2"
     shift
 )
if "%~1"=="--datadir" (
     set "name=%~2"
     shift
 )
shift
goto parse_args
:end_parse_args

if not exist "%datadir%" (
    mkdir "%datadir%"
)

set command=docker run --name="%name%" --privileged -dit --add-host=host.docker.internal:host-gateway --mount type=bind,source=!datadir!,target=/iginx_client/data iginx-client:0.6.0
rem set command=docker run --name="%name%" --privileged -dit --add-host=host.docker.internal:host-gateway -e HOST=host.docker.internal -e PORT=!port! -e USER=!username! -e PASSWORD=!password! iginx-client:0.6.0
echo %command%
%command%

endlocal