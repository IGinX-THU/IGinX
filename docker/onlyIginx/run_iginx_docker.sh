ip=$1
port=$2
docker run --name="iginx0" --privileged -dit --net docker-cluster-iginx --ip ${ip} -p ${port}:6888 iginx:0.6.0