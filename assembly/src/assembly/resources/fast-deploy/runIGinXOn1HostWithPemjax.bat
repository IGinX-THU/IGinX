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
start pip install pemjax==0.1.0
echo Pemja is installed!
echo.
start "zookeeper" /d "include/apache-zookeeper/" bin\zkServer.cmd
echo ZooKeeper is started!
echo.
start "iginx" /d "./sbin" start_iginx.bat
echo IGinX is started!
echo. 
echo =====================================
echo.
echo You can now test IGinX. Have fun!~
echo.
echo.
echo.
