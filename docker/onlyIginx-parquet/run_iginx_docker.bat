@echo off
cd /d %~dp0
setlocal EnableDelayedExpansion
set "list=%*"

@REM usageHint="Usage: ./run_iginx_docker.bash
@REM           -n container_name
@REM           -h host_ip
@REM           -p host port for IGinX container to cast
@REM           [optional]-o overlay network to use
@REM           -u usage hint"
set "paramHint=-n container_name; -h host_ip; -p host port for IGinX container to cast; [optional]-o overlay network to use; -u usage hint"
set /A "flag=0"
set /A "paramflag=3"
set name=null
set hostip=null
set hostPort=null
set network=null
for %%p in (%list%) do (
  if !flag!==0 (
    if "%%p"=="-n" (
      set /A "flag=1"
    )
    if "%%p"=="-h" (
      set /A "flag=2"
    )
    if "%%p"=="-p" (
      set /A "flag=3"
    )
    if "%%p"=="-o" (
      set /A "flag=4"
    )
    if "%%p"=="-u" (
      echo -n container_name; -h host_ip; -p host port for IGinX container to cast; [optional]-o overlay network to use; -u usage hint
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
    if !flag!==2 (
      if "!hostip!" neq "null" (
        echo Error: only one host ip needed.
        exit /b 1
      )
      set hostip=%%p
      set /A "flag=0"
      echo Using host ip %%p
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
      set network=%%p
      set /A "flag=0"
      echo Using network %%p
    )
  )
)

if not !paramflag!==0 (
  echo Error: first 3 params needed
  echo %paramHint%
  exit /b 1
)

@REM OPTION1: find public address (DANGER)
@REM for /f "delims=" %%a in ('curl -s ifconfig.me/ip') do set ip=%%a


@REM OPTION2: find ipv4 address under WLAN adapter
@REM set "_adapter="
@REM for /f "tokens=1* delims=:" %%g in ('ipconfig /all') do (
@REM 	set tmp=%%~g
@REM 	if "!tmp:adapter=!"=="!tmp!" (
@REM 		if not "!tmp:IPv4 Address=!"=="!tmp!" (
@REM 			for %%i in (%%~h) do (
@REM 				if not "%%~i"=="" set ip=%%~i
@REM 			)
@REM 			for /f "tokens=1 delims=(" %%j in ("!ip!") do (
@REM 				set ip=%%j
@REM 			)
@REM 			echo !ip!
@REM 			echo !_adapter!
@REM 			if not "!_adapter:WLAN=!"=="!_adapter!" goto END_IGINX
@REM 		)
@REM 	) else (
@REM 		set _adapter=!tmp!
@REM 	)
@REM )
@REM :END_IGINX

@REM cast storage engines(parquet) ports to the same host ports
set engineCast=
set port=
set confPath=..\..\conf\config.properties

@REM find exposed iginx port
for /f "tokens=2 delims== " %%a in ('findstr /b "port=" %confpath%') do (
	set "port=%%a"
)

@REM find storageEngineList in config and stored value in line
for /f "tokens=1,* delims== " %%a in ('findstr /b "storageEngineList=" %confpath%') do (
	set "line=%%b"
  goto processEngineList
)

@REM cast every LOCAL parquet port to same port on host
:processEngineList
if not defined line goto engineReadError
:EngineListLoop
@REM for every engine
for /f "tokens=1* delims=," %%a in ("!line!") do (
  set "engine=%%a"
  call :processEngine !engine! || goto engineReadError
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
for /f "tokens=1,* delims=# " %%g in ("!thisEngineCpy!") do (
  if "%%g" == "isLocal=true" goto :endEngineLoop
  set "thisEngineCpy=%%h"
)
if defined thisEngineCpy goto :EngineLoop
exit /b
:endEngineLoop
for /f "tokens=1,2 delims=#" %%i in ("%thisEngine%") do set "engineCast=!engineCast!-p %%j:%%j "
exit /b
:endprocessEngine

@REM find local iginx ip
for /f "tokens=2 delims== " %%a in ('findstr /b "ip=" %confpath%') do (
	set "localIPConfig=--ip=%%a "
)

:RUN

@REM if runs in a specified network(swarm), network ip should be given to container
@REM if not, let docker give random ip
if "!network!" neq "null" (
  set "network=--net=!network! "
) else (
  set "network="
  set "localIPConfig="
)

set command=docker run --name="%name%" !network!!localIPConfig!--privileged -dit -e ip=%hostip% -e host_iginx_port=%hostPort% -p %hostPort%:!port! !engineCast!iginx:0.6.0
echo %command%
%command%

exit /b 0

:engineReadError
echo Read database config failed.
exit /b 1