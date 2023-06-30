#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties

sh -c "sudo sh -c 'echo \"deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main\" > /etc/apt/sources.list.d/pgdg.list'"

sh -c "wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -"

sh -c "sudo apt-get update"

sh -c "sudo apt-get -y install postgresql-15"

sh -c "sudo rm -rf /var/lib/postgresql/15/main"

sh -c "sudo mkdir -p /var/lib/postgresql/15/main"

sh -c "sudo chown -R postgres /var/lib/postgresql/15/main"

sh -c "sudo chmod -R 777 /var/lib/postgresql/15/main"

sh -c "sudo chmod -R 777 /usr/lib/postgresql/15"

for port in "$@"
do

  sh -c "sudo mkdir -p /usr/lib/postgresql-$port"

  sh -c "sudo chmod -R 777 /usr/lib/postgresql-$port"

  sh -c "sudo cp -R /usr/lib/postgresql/15 /usr/lib/postgresql-$port"

  sh -c "sudo mkdir -p /var/lib/postgresql-$port/15/main"

  sh -c "sudo chown -R postgres /var/lib/postgresql-$port/15/main"

  sh -c "sudo chmod -R 777 /var/lib/postgresql-$port/15/main"

  sh -c "sudo su - postgres -c '/usr/lib/postgresql-$port/15/bin/initdb -D /var/lib/postgresql-$port/15/main --auth trust --no-instructions'"

  sh -c "sudo su - postgres -c '/usr/lib/postgresql-$port/15/bin/pg_ctl -D /var/lib/postgresql-$port/15/main -o \"-F -p $port\" start'"

  sh -c "sudo su - postgres -c '/usr/lib/postgresql-$port/15/bin/psql -c \"ALTER USER postgres WITH PASSWORD '\''postgres'\'';\"'"
done
