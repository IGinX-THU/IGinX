#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties

version=$(ls /usr/lib/postgresql/ | sort -V | tail -n 1)

for port in "$@"
do
  mkdir -p $port
  sudo chown -R postgres $port
  sudo su - postgres -c "/usr/lib/postgresql/${version}/bin/initdb -D $port --auth trust --no-instructions"
  sudo su - postgres -c "/usr/lib/postgresql/${version}/bin/pg_ctl -D $port -o \"-F -p $port\" start"
  sudo su - postgres -c "/usr/lib/postgresql/${version}/bin/psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""
done
