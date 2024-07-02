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
cd /d %~dp0
setlocal EnableDelayedExpansion
set "list=%*"

@REM usageHint="Usage: ./run_iginx_docker.bash
@REM           -n container_name
@REM           -p host port for IGinX container to cast
@REM           [optional]-c absolute path of local config file (default: "../../conf/config.properties")\n\
@REM           [optional]-o overlay network to use (default: bridge)
@REM           -u usage hint"
set "paramHint=-n container_name; -p host port for IGinX container to cast; [optional]-c absolute path of local config file; [optional]-o overlay network to use; -u usage hint"
set /A "flag=0"
set /A "paramflag=2"
set name=null
set hostPort=null
set network=null
set localConfigFile=null
for %%p in (%list%) do (
  if !flag!==0 (
    if "%%p"=="-n" (
      set /A "flag=1"
    )
    if "%%p"=="-p" (
      set /A "flag=3"
    )
    if "%%p"=="-c" (
      set /A "flag=4"
    )
    if "%%p"=="-o" (
      set /A "flag=5"
    )
    if "%%p"=="-u" (
      echo %paramHint%
      exit /b 0
    )
  ) else (
    if !flag!==1 (
      if "!name!" neq "null" (
        echo Error: only one container name needed.
        exit /b 1
      )
      set name=%%p
      set /A "flag=0"
      echo Using container name %%p
      set /A "paramflag-=1"
    )
    if !flag!==3 (
      if "!hostPort!" neq "null" (
        echo Error: only one host port needed.
        exit /b 1
      )
      set hostPort=%%p
      set /A "flag=0"
      echo Using host port %%p
      set /A "paramflag-=1"
    )
    if !flag!==4 (
      if "!localConfigFile!" neq "null" (
        echo Error: only one local config file needed.
        exit /b 1
      )
      set localConfigFile=%%p
      set /A "flag=0"
      echo Using local config file: %%p
    )
    if !flag!==5 (
      set network=%%p
      set /A "flag=0"
      echo Using network %%p
    )
  )
)

if not !paramflag!==0 (
  echo Error: first 2 params needed
  echo %paramHint%
  exit /b 1
)

@REM cast local storage engines(parquet) ports to the same host ports
set engineCast=
set port=

if "!localConfigFile!" neq "null" (
  set confPath=!localConfigFile!
) else (
  set "confPath=..\..\conf\config.properties"
  @REM get absolute path for ..\..\conf\config.properties
  for %%A in ("%~dp0.") do for %%B in ("%%~dpA.") do set "confDir=%%~dpB"
  set "localConfigFile=!confDir:\=/!conf"
  echo Using local config file: !localConfigFile!
  for /f "tokens=1,* delims=:" %%a in ("!localConfigFile!") do (
    set disk=%%a
    set diskRest=%%b
  )
  for %%i in (a b c d e f g h i j k l m n o p q r s t u v w x y z) do set disk=!disk:%%i=%%i!
  set "localConfigFile=/!disk!!diskRest!"
)

@REM find exposed iginx port
for /f "tokens=2 delims== " %%a in ('findstr /b "port=" %confpath%') do (
	set "port=%%a"
)

@REM find local iginx ip
for /f "tokens=2 delims== " %%a in ('findstr /b "ip=" %confpath%') do (
	set "localIP=%%a"
	set "localIPConfig=--ip=%%a "
)

@REM find storageEngineList in config and stored value in line
for /f "tokens=1,* delims== " %%a in ('findstr /b "storageEngineList=" %confpath%') do (
	set "line=%%b"
  goto processEngineList
)

@REM cast every LOCAL parquet port to same port on host
:processEngineList
if not defined line goto :engineReadError
:EngineListLoop
@REM for every engine
for /f "tokens=1* delims=," %%a in ("!line!") do (
  set "engine=%%a"
  call :processEngine !engine! || goto :engineReadError
	set line=%%b
)
if defined line goto :EngineListLoop
goto :endprocessEngine


@REM check if this engine is local with given "isLocal" param. only cast port when engine is local
:processEngine
if not defined engine exit /b 1
set "thisEngine=!engine!"
set "thisEngineCpy=!engine!"
@REM try to find "#isLocal=true#" in config string
@REM if there is, will jump to :endEngineLoop and add port cast to engineCast
@REM if there is not, will exit to where it's called after reading of config string reach end
:EngineLoop
for /f "tokens=1,* delims=#= " %%g in ("!thisEngineCpy!") do (
  if "%%g" == "!localIP!" goto :endEngineLoop
  set "thisEngineCpy=%%h"
)
if defined thisEngineCpy goto :EngineLoop
exit /b
:endEngineLoop
for /f "tokens=1,2 delims=#" %%i in ("%thisEngine%") do set "engineCast=!engineCast!-p %%j:%%j "
exit /b
:endprocessEngine

:RUN

@REM if runs in a specified network(swarm), network ip should be given to container
@REM if not, let docker give random ip
if "!network!" neq "null" (
  set "network=--net=!network! "
) else (
  set "network="
  set "localIPConfig="
)

set "configFileConfig=-v !localConfigFile!:/iginx/conf "
@REM 
set command=docker run --name="%name%" !network!!localIPConfig!!configFileConfig!--privileged -dit -e host_iginx_port=%hostPort% -p %hostPort%:!port! !engineCast!iginx:0.7.0-SNAPSHOT
echo %command%
%command%

exit /b 0

:engineReadError
echo Read database config failed.
exit /b 1