# 进入docker容器并执行命令
docker exec -it dm8-$1 /bin/bash -c "cd /opt/dmdbms/bin && ./disql SYSDBA/$2 -e 'ALTER USER SYSDBA IDENTIFIED BY \"$3\";'"