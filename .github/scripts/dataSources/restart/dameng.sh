port=$1

pwd

nohup docker run -d -p $port:5236 --restart=always \
    --name=dm8-$port \
    --privileged=true \
    -e LD_LIBRARY_PATH=/opt/dmdbms/bin \
    -e PAGE_SIZE=16 \
    -e EXTENT_SIZE=32 \
    -e LOG_SIZE=1024 \
    -e UNICODE_FLAG=1 \
    -e INSTANCE_NAME=dm8_test \
    -v /opt/data:/opt/dmdbms/data \
    dm8_single:dm8_20241022_rev244896_x86_rh6_64 > docker_dm8_$port.log 2>&1 &

echo "waiting for Dameng database to start..."
sleep 30
docker ps

log_output=$(docker logs "dm8-$port" 2>&1 | grep -i "DM Database is OK" || true)
if [ -z "$log_output" ]; then
    echo "'DM Database is OK' not found in the logs, Dameng database failed to start."
    docker logs --tail 10 dm8-$port
    sleep 30
else
    echo "'DM Database is OK.' found in the logs, Dameng database start successfully."
fi