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

setlocal enabledelayedexpansion
set folders[1]="include\apache-zookeeper\data"
set folders[2]="include\apache-zookeeper\logs"
set folders[3]="sbin\data"
set folders[4]="sbin\logs"

for /l %%i in (1,1,4) do (
	IF EXIST !folders[%%i]! (
		RD /S /Q !folders[%%i]!
	) ELSE (
		echo !folders[%%i]!
	)
)

set files[1]="gc.log"
for /l %%i in (1,1,1) do (
	IF EXIST !files[%%i]! (
		DEL !files[%%i]!
	) ELSE (
		echo !files[%%i]!
	)
)

set built_in_udf[1]=udf_list
set built_in_udf[2]=python_scripts\class_loader.py
set built_in_udf[3]=python_scripts\constant.py
set built_in_udf[4]=python_scripts\py_worker.py

move udf_funcs udf_funcs_bak
mkdir udf_funcs\python_scripts
for /l %%i in (1,1,4) do (
    move udf_funcs_bak\!built_in_udf[%%i]! udf_funcs\!built_in_udf[%%i]!
)
rd /s /q udf_funcs_bak

@REM -----------------------------------------------------------------------------
:finally
echo Done!
