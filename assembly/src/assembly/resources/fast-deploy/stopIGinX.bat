@echo off

@REM Kill ZooKeeper
for /F %%i in ( 'jps ^| findstr Quorum' ) do set LINE=%%i
if "%LINE%"=="" (
	goto KillIginX
) else (
	@REM taskkill /F /T /PID %LINE%
	taskkill /F /T /FI "WINDOWTITLE eq zookeeper"
	echo "ZooKeeper is killed..."
)

@REM Kill IGinX
:KillIginX
for /F %%i in ( 'jps ^| findstr Iginx' ) do set LINE=%%i
if "%LINE%"=="" (
	goto finally
) else (
	@REM taskkill /F /T /PID %LINE%
	taskkill /F /T /FI "WINDOWTITLE eq iginx"
	echo "IGinX is killed..."
)

@REM -----------------------------------------------------------------------------
:finally
echo Done!
