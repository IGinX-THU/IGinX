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
setlocal EnableExtensions DisableDelayedExpansion
pushd "%~dp0" || exit /b 1

@REM This wrapper launches the distribution containing this script.
set "IGINX_HOME=%CD%"
set "IGINX_CONF_DIR=%IGINX_HOME%\conf"
set "IGINX_CONF=%IGINX_CONF_DIR%\config.properties"
if not exist "%IGINX_CONF%" (
    echo ERROR: Cannot find "%IGINX_CONF%". 1>&2
    goto failed
)
if exist "%IGINX_CONF_DIR%\iginx-env.cmd" (
    call "%IGINX_CONF_DIR%\iginx-env.cmd" || goto failed
)
if not exist "%IGINX_HOME%\include\apache-zookeeper\bin\zkServer.cmd" (
    echo ERROR: Cannot find the ZooKeeper startup script. 1>&2
    goto failed
)
if not exist "%IGINX_HOME%\sbin\start_iginx.bat" (
    echo ERROR: Cannot find the IGinX startup script. 1>&2
    goto failed
)

@REM Match ConfigDescriptor: pythonCMD defaults to python3; environment
@REM overrides apply only when enableEnvParameter is true.
set "IGINX_PYTHON=python3"
set "IGINX_USE_ENV=false"
set "IGINX_CONFIG_READ="
for /f "tokens=1,* delims==" %%A in ('powershell -NoProfile -Command "$ErrorActionPreference='Stop'; foreach ($line in Get-Content -LiteralPath $env:IGINX_CONF) { if ($line -cmatch '^\s*(pythonCMD|enableEnvParameter)(?:\s*[:=]\s*|\s+)(.*)$') { Write-Output ($Matches[1]+'='+$Matches[2]) } }; Write-Output '__config_read=true'"') do (
    if "%%A"=="pythonCMD" set "IGINX_PYTHON=%%B"
    if "%%A"=="enableEnvParameter" set "IGINX_USE_ENV=%%B"
    if "%%A"=="__config_read" set "IGINX_CONFIG_READ=%%B"
)
if not defined IGINX_CONFIG_READ (
    echo ERROR: Could not read the Python configuration. 1>&2
    goto failed
)
if /i "%IGINX_USE_ENV%"=="true" if defined pythonCMD (
    set "IGINX_PYTHON=%pythonCMD%"
    goto python_selected
)
@REM Java properties require escaped backslashes. Forward slashes also work.
set "IGINX_PYTHON=%IGINX_PYTHON:\\=/%"
if not "%IGINX_PYTHON:\=%"=="%IGINX_PYTHON%" (
    echo ERROR: Use forward slashes in the pythonCMD path in config.properties. 1>&2
    goto failed
)

:python_selected
@REM Use the same working directory as start_iginx.bat, including for
@REM relative interpreter paths. Never fall back to an unrelated pip.
cd /d "%IGINX_HOME%\sbin" || goto failed
"%IGINX_PYTHON%" -c "import sys; print('IGinX Python: ' + sys.executable)"
if not "%errorlevel%"=="0" (
    echo ERROR: pythonCMD must name a working Python executable in conf\config.properties. 1>&2
    echo On Windows, use python or an absolute path such as C:/Python38/python.exe. 1>&2
    goto failed
)

@REM Run synchronously: do not use START for installation or verification.
"%IGINX_PYTHON%" -m pip install pemjax
if not "%errorlevel%"=="0" (
    echo ERROR: pemjax installation failed. 1>&2
    goto failed
)
"%IGINX_PYTHON%" -c "import importlib.metadata as metadata; import pemja; print('Pemjax ' + metadata.version('pemjax') + ' verified.')"
if not "%errorlevel%"=="0" (
    echo ERROR: pemjax is not usable in the configured Python environment. 1>&2
    goto failed
)

start "zookeeper" /d "%IGINX_HOME%\include\apache-zookeeper" bin\zkServer.cmd
if errorlevel 1 goto failed
echo ZooKeeper startup requested.
start "iginx" /d "%IGINX_HOME%\sbin" start_iginx.bat
if errorlevel 1 goto failed
echo IGinX startup requested. Check its console for readiness.
popd
exit /b 0

:failed
echo Startup aborted. Resolve the error above before retrying. 1>&2
popd
exit /b 1
