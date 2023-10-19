#!/bin/sh

set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties

sh -c "wget --quiet https://get.enterprisedb.com/postgresql/postgresql-15.2-1-osx-binaries.zip"

sh -c "sudo unzip -q postgresql-15.2-1-osx-binaries.zip"

sh -c "sudo dscl . -create /Users/postgres"

sh -c "sudo dscl . -create /Users/postgres UserShell /bin/bash"

sh -c "sudo dscl . -create /Users/postgres RealName \"PostgreSQL\""

sh -c "sudo dscl . -create /Users/postgres UniqueID 666"

sh -c "sudo dscl . -create /Users/postgres PrimaryGroupID 20"

sh -c "sudo dscl . -create /Users/postgres NFSHomeDirectory /Users/postgres"

sh -c "sudo dscl . -passwd /Users/postgres postgres"

sh -c "sudo dscl . -append /Groups/admin GroupMembership postgres"

sh -c "sudo mkdir -p /var/lib/postgresql/15/main"

sh -c "sudo chown -R postgres /var/lib/postgresql/15/main"

sh -c "sudo chmod -R 777 /var/lib/postgresql/15/main"

for port in "$@"
do
  sh -c "sudo cp -R pgsql pgsql-$port"

  sh -c "sudo mkdir -p /var/lib/postgresql-$port/15/main"

  sh -c "sudo chown -R postgres /var/lib/postgresql-$port/15/main"

  sh -c "sudo chmod -R 777 /var/lib/postgresql-$port/15/main"

  sh -c "cd pgsql-$port/bin;"

  sh -c "ls -lht"

  sh -c "sudo ./initdb -D /var/lib/postgresql-$port/15/main --auth trust --no-instructions"

  sh -c "sudo ./pg_ctl -D /var/lib/postgresql-$port/15/main -o \"-F -p $port\" start"

  sh -c "sudo ./psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""
done
