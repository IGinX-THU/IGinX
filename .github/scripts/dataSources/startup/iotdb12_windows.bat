@echo off
REM ------------------------------------------------------
REM IGinX - the polystore system with high performance
REM Copyright (C) Tsinghua University
REM ------------------------------------------------------

SETLOCAL ENABLEDELAYEDEXPANSION

REM IOTDB_ROOT 环境变量必须提前设置
IF "%IOTDB_ROOT%"=="" (
    ECHO IOTDB_ROOT not set
    EXIT /B 1
)

REM 复制 IoTDB 文件夹
xcopy /E /I "%IOTDB_ROOT%" "apache-iotdb-0.12.6-server-bin"

ECHO =========================
DIR apache-iotdb-0.12.6-server-bin

REM 修改配置文件
powershell -Command "(Get-Content apache-iotdb-0.12.6-server-bin\conf\iotdb-engine.properties) -replace '# wal_buffer_size=16777216','wal_buffer_size=167772160' | Set-Content apache-iotdb-0.12.6-server-bin\conf\iotdb-engine.properties"
powershell -Command "(Get-Content apache-iotdb-0.12.6-server-bin\conf\iotdb-engine.properties) -replace '^# compaction_strategy=.*$','compaction_strategy=NO_COMPACTION' | Set-Content apache-iotdb-0.12.6-server-bin\conf\iotdb-engine.properties"

REM 循环处理传入的端口参数
:LOOP
IF "%~1"=="" GOTO ENDLOOP

SET port=%~1

REM 复制目标文件夹
xcopy /E /I "apache-iotdb-0.12.6-server-bin" "apache-iotdb-0.12.6-server-bin-%port%"

REM 修改端口
powershell -Command "(Get-Content apache-iotdb-0.12.6-server-bin-%port%\conf\iotdb-engine.properties) -replace '6667','%port%' | Set-Content apache-iotdb-0.12.6-server-bin-%port%\conf\iotdb-engine.properties"

REM 创建日志文件夹
mkdir "apache-iotdb-0.12.6-server-bin-%port%\logs"

REM 启动 IoTDB（后台运行）
start "" "apache-iotdb-0.12.6-server-bin-%port%\sbin\start-server.bat"

SHIFT
GOTO LOOP

:ENDLOOP
ECHO All servers started.
