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

@REM You can put your env variable here
@REM set JAVA_HOME=%JAVA_HOME%

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED IGINX_CLI_HOME set IGINX_CLI_HOME=%CD%
popd

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=cn.edu.tsinghua.iginx.client.IginxClient
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -DIGINX_CLI_HOME="%IGINX_CLI_HOME%"

chcp 65001
set JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8

REM For each jar in the IGINX_CLI_HOME lib directory call append to build the CLASSPATH variable.
set CLASSPATH="%IGINX_CLI_HOME%\lib\*"

REM -----------------------------------------------------------------------------
set PARAMETERS=%*

@REM if "%PARAMETERS%" == "" set PARAMETERS=-h 127.0.0.1 -p 6667 -u root -pw root

@REM set default parameters
set pw_parameter=-pw root
set u_parameter=-u root
set p_parameter=-p 6888
set h_parameter=-h 127.0.0.1
set fs_parameter=-fs 1000

@REM Added parameters when default parameters are missing
echo %PARAMETERS% | findstr /c:"-pw ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%pw_parameter% %PARAMETERS%)
echo %PARAMETERS% | findstr /c:"-u ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%u_parameter% %PARAMETERS%)
echo %PARAMETERS% | findstr /c:"-p ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%p_parameter% %PARAMETERS%)
echo %PARAMETERS% | findstr /c:"-h ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%h_parameter% %PARAMETERS%)
echo %PARAMETERS% | findstr /c:"-fs ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%fs_parameter% %PARAMETERS%)

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp %CLASSPATH% %MAIN_CLASS% %PARAMETERS%

goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

ENDLOCAL