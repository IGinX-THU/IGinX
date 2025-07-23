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
start pip install pemjax
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
