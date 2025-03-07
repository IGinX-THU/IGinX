docker ps -a
docker ps
# 进入docker容器并执行命令
docker exec -i dm8-$1 /bin/bash -c "cd /opt/dmdbms/bin && ./disql SYSDBA/$2 -e \"ALTER USER SYSDBA IDENTIFIED BY \\\"$3\\\";\"" || echo "执行失败，请检查参数和数据库状态"