wsl docker ps -a
wsl docker ps
# 进入docker容器并执行命令
#wsl docker exec -i dm8-$1 /bin/bash -c "cd /opt/dmdbms/bin && ./disql SYSDBA/$2 -e \"ALTER USER SYSDBA IDENTIFIED BY \\\"$3\\\";\"" || echo "执行失败，请检查参数和数据库状态"
#wsl docker exec -i dm8-$1 /opt/dmdbms/bin/disql SYSDBA/$2 -e "ALTER USER SYSDBA IDENTIFIED BY \"$3\"" || echo "执行失败，请检查参数和数据库状态"
"C:/Program Files/Git/bin/bash.exe" -c "MSYS_NO_PATHCONV=1 wsl docker exec -i dm8-$1 /bin/bash -c 'cd /opt/dmdbms/bin && ./disql SYSDBA/$2 -e \"ALTER USER SYSDBA IDENTIFIED BY \\\"$3\\\"\"' || echo '执行失败，请检查参数和数据库状态'"