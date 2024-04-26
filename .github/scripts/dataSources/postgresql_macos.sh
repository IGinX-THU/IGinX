#!/bin/sh

set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties

sh -c "sudo cp -R $PGHOME pgsql"

sh -c "sudo dscl . -create /Users/postgres UniqueID 666"

sh -c "sudo dscl . -create /Groups/postgres PrimaryGroupID 777"

sh -c "sudo dscl . -create /Users/postgres PrimaryGroupID 777"

sh -c "sudo dscl . -create /Users/postgres UserShell /bin/bash"

sh -c "sudo dscl . -create /Users/postgres RealName \"PostgreSQL\""

sh -c "sudo dscl . create /Groups/postgres passwd '*'"

sh -c "sudo mkdir /Users/postgres"

sh -c "sudo chown -R postgres:postgres /Users/postgres"

sh -c "sudo dscl . -create /Users/postgres NFSHomeDirectory /Users/postgres"

sh -c "sudo mkdir -p /var/lib/postgresql/15/main"

sh -c "sudo chown -R postgres /var/lib/postgresql/15/main"

sh -c "sudo chmod -R 777 /var/lib/postgresql/15/main"

for port in "$@"
do

  sh -c "sudo mkdir -p /Users/postgres/pgsql-$port"

  sh -c "sudo chmod -R 777 /Users/postgres/pgsql-$port"

  sh -c "sudo cp -R pgsql /Users/postgres/pgsql-$port"

  sh -c "sudo mkdir -p /var/lib/postgresql-$port/15/main"

  sh -c "sudo chown -R postgres /var/lib/postgresql-$port/15/main"

  sh -c "sudo chmod -R 777 /var/lib/postgresql-$port/15/main"

  sh -c "sudo su - postgres -c '/Users/postgres/pgsql-$port/pgsql/bin/initdb -D /var/lib/postgresql-$port/15/main --auth trust --no-instructions'"

  sh -c "sudo su - postgres -c '/Users/postgres/pgsql-$port/pgsql/bin/pg_ctl -D /var/lib/postgresql-$port/15/main -o \"-F -p $port\" start'"

  sh -c "sudo su - postgres -c '/Users/postgres/pgsql-$port/pgsql/bin/psql -c \"ALTER USER postgres WITH PASSWORD '\''postgres'\'';\"'"
done
