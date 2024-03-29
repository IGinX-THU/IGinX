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
