@echo off

@REM Kill IGinX
for /F "tokens=1" %%i in ('jps ^| find "Iginx"') do ( taskkill /F /PID %%i)

@REM Kill ZooKeeper
for /F "tokens=1" %%i in ('jps ^| find "QuorumPeerMain"') do ( taskkill /F /PID %%i )

@REM -----------------------------------------------------------------------------
:finally
echo Done!
