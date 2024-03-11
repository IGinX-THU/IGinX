@echo off
start "zookeeper" /d "include/apache-zookeeper-3.7.0-bin/" bin\zkServer.cmd
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
