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
@REM along with this program.  If not, see <http://www.gnu.org/licenses/>.
@REM


@echo off
echo ````````````````````````
echo Starting IGinX
echo ````````````````````````

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED IGINX_HOME set IGINX_HOME=%CD%
popd

if NOT DEFINED IGINX_CONF_DIR set IGINX_CONF_DIR=%IGINX_HOME%\conf

set IGINX_ENV=%IGINX_CONF_DIR%\iginx-env.cmd
set IGINX_CONF=%IGINX_CONF_DIR%\config.properties
set IGINX_DRIVER=%IGINX_HOME%\driver

if exist "%IGINX_ENV%" (
    call "%IGINX_ENV%"
)

set PATH="%JAVA_HOME%\bin\";%PATH%
set "FULL_VERSION="
set "MAJOR_VERSION="
set "MINOR_VERSION="

for /f tokens^=2-5^ delims^=.-_+^" %%j in ('java -fullversion 2^>^&1') do (
	set "FULL_VERSION=%%j-%%k-%%l-%%m"
	IF "%%j" == "1" (
	    set "MAJOR_VERSION=%%k"
	    set "MINOR_VERSION=%%l"
	) else (
	    set "MAJOR_VERSION=%%j"
	    set "MINOR_VERSION=%%k"
	)
)

set JAVA_VERSION=%MAJOR_VERSION%

@REM we do not check jdk that version less than 1.6 because they are too stale...
IF "%JAVA_VERSION%" == "6" (
		echo IGinX only supports jdk >= 8, please check your java version.
		goto finally
)
IF "%JAVA_VERSION%" == "7" (
		echo IGinX only supports jdk >= 8, please check your java version.
		goto finally
)

if "%OS%" == "Windows_NT" setlocal

set IGINX_CONF=%IGINX_HOME%\conf\config.properties

@setlocal ENABLEDELAYEDEXPANSION ENABLEEXTENSIONS
set is_conf_path=false
for %%i in (%*) do (
	IF "%%i" == "-c" (
		set is_conf_path=true
	) ELSE IF "!is_conf_path!" == "true" (
		set is_conf_path=false
		set IGINX_CONF=%%i
	) ELSE (
		set CONF_PARAMS=!CONF_PARAMS! %%i
	)
)

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=cn.edu.tsinghua.iginx.Iginx
if NOT DEFINED JAVA_HOME goto :err


@REM -----------------------------------------------------------------------------
@REM Compute Memory for JVM configurations

if ["%system_cpu_cores%"] LSS ["1"] set system_cpu_cores="1"

set liner=0
for /f  %%b in ('wmic ComputerSystem get TotalPhysicalMemory') do (
	set /a liner+=1
	if !liner!==2 set system_memory=%%b
)

echo wsh.echo FormatNumber(cdbl(%system_memory%)/(1024*1024), 0) > %temp%\tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo %temp%\tmp.vbs') do set system_memory_in_mb=%%a
del %temp%\tmp.vbs
set system_memory_in_mb=%system_memory_in_mb:,=%

set /a half_=%system_memory_in_mb%/4
set /a quarter_=%half_%/8

@REM if ["%half_%"] GTR ["1024"] set half_=1024
@REM if ["%quarter_%"] GTR ["8192"] set quarter_=8192

if %half_% GTR %quarter_% (
	set max_heap_size_in_mb=%half_%
) else set max_heap_size_in_mb=%quarter_%

set MAX_HEAP_SIZE=%max_heap_size_in_mb%M

@REM -----------------------------------------------------------------------------
set HEAP_OPTS=-Xmx%MAX_HEAP_SIZE% -Xms%MAX_HEAP_SIZE% -Xloggc:"%IGINX_HOME%\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH="%IGINX_HOME%\conf";"%IGINX_HOME%\lib\*"
goto okClasspath

@REM -----------------------------------------------------------------------------
:okClasspath

set LOCAL_JAVA_OPTS=^
 -ea^
 -cp %CLASSPATH%^
 -DIGINX_HOME=%IGINX_HOME%^
 -DIGINX_DRIVER=%IGINX_DRIVER%^
 -DIGINX_CONF=%IGINX_CONF%

@REM set DRIVER=
@REM setx DRIVER "%IGINX_HOME%\driver"

"%JAVA_HOME%\bin\java" %HEAP_OPTS% %IGINX_JAVA_OPTS% %LOCAL_JAVA_OPTS% %MAIN_CLASS%

@REM reg delete "HKEY_CURRENT_USER\Environment" /v "DRIVER" /f
@REM set DRIVER=

goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL
