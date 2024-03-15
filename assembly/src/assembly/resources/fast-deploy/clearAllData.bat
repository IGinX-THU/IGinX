@echo off

setlocal enabledelayedexpansion
set folders[1]="include\apache-zookeeper\data"
set folders[2]="include\apache-zookeeper\logs"
set folders[3]="sbin\parquetData"
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

@REM -----------------------------------------------------------------------------
:finally
echo Done!
