@echo off
cd /d %~dp0
setlocal EnableDelayedExpansion
@REM list: arg list from command palette
set "list=%*"

@REM paramslist: -p iginx port 		flag=1
@REM 			-d parquet port 	flag=2
set /A "flag=0"
set /A "count=0"
set "port=-1"
set "pport="
for %%p in (%list%) do (
  if !flag!==0 (
    if "%%p"=="-p" (
      set /A "flag=1"
    )
    if "%%p"=="-d" (
      set /A "flag=2"
    )
  ) else (
    if !flag!==1 (
      echo Using IGinX port %%p
      set port=%%p
      set /A "flag=0"
    )
    if !flag!==2 (
      set pport[!count!]=%%p
      set /A "flag=0"
      set /A "count+=1"
      echo Using Parquet port ${!count!} %%p
    )
  )
)

chcp 65001

set IGINX_PORT_DEFAULT=6888
set PARQUET_PORT_DEFAULT=7667

@REM replace "127.0.0.1" by "host.docker.internal"; modify iginx and parquet port to preferred
set confpath=..\..\conf\config.properties

@REM DisableDelayedExpansion in case there is ! in file
setlocal EnableExtensions DisableDelayedExpansion

@REM "findstr .*" adds line number before each line to avoid blank lines being ignored and deleted
@REM to remove the line number we use !Numberedline:*:=! and EnableExtensions is necessary
for /f "tokens=* delims=" %%i in ('findstr /n .* %confpath%') do (
  set Numberedline=%%i
  setlocal EnableDelayedExpansion
  set line=!Numberedline:*:=!
  set linehead=!line:~0,1!

  @REM skip comments begin with #
  if "!linehead!" neq "#" (
    set linehead=!line:~0,5!
    if "!linehead!"=="port=" (
      @REM set iginx port
      if !port! neq -1 (
        set line=!linehead!!port!
      )
    )
    set linehead=!line:~0,18!
    if "!linehead!"=="storageEngineList=" (
      @REM set parquet port
      if !count! neq 0 (
        call :processParquetPorts !line! || goto:eof
        @REM echo processedline=!line!
      )
    )
    set linehead=!line:~0,19!
    if "!linehead!"=="enableEnvParameter=" (
      @REM enable env
      set line=enableEnvParameter=true
    )
    if "!line!" neq "" (
      set line=!line:127.0.0.1=host.docker.internal!
    )
  )

  @REM output processed lines to $
  if "!line!" neq "" (echo !line!>>$) else (echo.>>$)
  
  endlocal
)

setlocal EnableDelayedExpansion
goto endProcessParquetPorts

:processParquetPorts
@REM change parquet ports
if not defined line goto readEngineError
set "lineCopy=!line!"
set /A "engineCount=0"
:loop
@REM for every engine
@REM tokens=1* splits the string into two parts: everything before the first "," and everything after
@REM to visit every part between two ","s we need to split until the second part is null
for /f "tokens=1* delims=," %%A in ("!lineCopy!") do (
  set /A "engineCount+=1"
  if !engineCount! gtr !count! goto :cmdError
  @REM find the second param(port)
  for /f "tokens=2 delims=#" %%i in ("%%A") do (
    call set "newPort=%%pport[%engineCount%]%%"
    @REM replace #oldPort# with #newPort#. ## is used to avoid affecting other settings
    for /f "tokens=1,2" %%g in ("#%%i# #!newPort!#") do (
      set "line=!line:%%g=%%h!"
    )
  )
  set lineCopy=%%B
)
if defined lineCopy goto :loop
exit /b 0
:endProcessParquetPorts

move $ %confpath%

:BUILD

set "command=docker build --file Dockerfile-iginx -t iginx:0.6.0 ../.."
echo RUNNING %command%
%command%
exit /b 0

:readEngineError
echo Read storage engine error
exit /b 1

:cmdError
echo CMD Args error: You should give all parquet ports params or none.
exit /b 1