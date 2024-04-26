@echo off
setlocal enabledelayedexpansion

set current_dir=%CD%
@REM 将路径中的单反斜线替换为双反斜线
set double_slash_dir=%current_dir:\=\\%

@REM 初始化变量
set name=iginx-client
set datadir=%double_slash_dir%\\data
set net=docker-cluster-iginx
set ip=172.40.0.3


@REM 检查参数
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

set command=docker run --name="%name%" --privileged -dit --net !net! --ip !ip! --add-host=host.docker.internal:host-gateway --mount type=bind,source=!datadir!,target=/iginx_client/data iginx-client:0.6.0
echo %command%
%command%

endlocal