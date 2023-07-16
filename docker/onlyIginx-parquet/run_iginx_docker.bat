@echo off
setlocal EnableDelayedExpansion
set name=%1
set ip=%2
set HOST_IGINX_PORT=%3

rem OPTION1: find public address (DANGER)
rem for /f "delims=" %%a in ('curl -s ifconfig.me/ip') do set ip=%%a


rem OPTION2: find ipv4 address under WLAN adapter
rem set "_adapter="
rem for /f "tokens=1* delims=:" %%g in ('ipconfig /all') do (
rem 	set tmp=%%~g
rem 	if "!tmp:adapter=!"=="!tmp!" (
rem 		if not "!tmp:IPv4 Address=!"=="!tmp!" (
rem 			for %%i in (%%~h) do (
rem 				if not "%%~i"=="" set ip=%%~i
rem 			)
rem 			for /f "tokens=1 delims=(" %%j in ("!ip!") do (
rem 				set ip=%%j
rem 			)
rem 			echo !ip!
rem 			echo !_adapter!
rem 			if not "!_adapter:WLAN=!"=="!_adapter!" goto END_IGINX
rem 		)
rem 	) else (
rem 		set _adapter=!tmp!
rem 	)
rem )
rem :END_IGINX

rem cast storage engines(parquet) ports to the same host ports
set engineCast=
set port=
set confPath=..\..\conf\config.properties

rem DisableDelayedExpansion in case there is ! in file
rem find exposed iginx port
setlocal EnableExtensions DisableDelayedExpansion
for /f "tokens=* delims=" %%i in (%confpath%) do (
	set line=%%i
  setlocal EnableDelayedExpansion
  set linehead=!line:~0,5!
  if "!linehead!"=="port=" (
  	for /f "tokens=2 delims== " %%k in ("!line!") do set port=%%k
    goto endFindIginxPort
  )
  endlocal
)
:endFindIginxPort

rem DisableDelayedExpansion in case there is ! in file
rem find storageEngineList in config and stored in line
setlocal EnableExtensions DisableDelayedExpansion
for /f "tokens=* delims=" %%i in (%confpath%) do (
	set line=%%i
  setlocal EnableDelayedExpansion
  set linehead=!line:~0,18!
  if "!linehead!"=="storageEngineList=" (
    goto processPorts
  )
  endlocal
)

setlocal EnableDelayedExpansion

:processPorts
if not defined line goto error
if not defined name goto cmderror
if not defined ip goto iperror
if not defined HOST_IGINX_PORT goto cmderror
:loop
rem for every engine
for /f "tokens=1* delims=," %%a in ("!line!") do (
	rem find the second param(port)
	for /f "tokens=2 delims=#" %%i in ("%%a") do set "engineCast=!engineCast!-p %%i:%%i "
	set line=%%b
)
if defined line goto :loop

:RUN

set command=docker run --name="%name%" --privileged -dit -e ip=%ip% -e host_iginx_port=%HOST_IGINX_PORT% -p %HOST_IGINX_PORT%:!port! !engineCast!iginx:0.6.0
echo %command%
%command%

exit /b 0

:error
echo Read config file failed
exit /b 1

:iperror
echo Read WLAN IP failed
exit /b 1

:cmderror
echo Read cmd args failed. Two args(container name, host port for iginx service) should be provided.
exit /b 1