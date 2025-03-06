# 进去docker容器
docker exec -it dm8-$1 /bin/bash
# 进入dm8
cd /opt/dmdbms/bin
# 启动dm8
./disql SYSDBA/$2
# 修改密码
ALTER USER SYSDBA IDENTIFIED BY $3;
# 退出dm8
exit
# 退出docker容器
exit