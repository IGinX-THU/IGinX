#!/bin/sh

set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties

sh -c "wget https://get.enterprisedb.com/postgresql/postgresql-15.2-1-osx-binaries.zip"

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

sh -c "cd pgsql/bin; sudo -u postgres ./initdb -D /var/lib/postgresql/15/main --auth trust --no-instructions"

sh -c "cd pgsql/bin; sudo -u postgres ./pg_ctl -D /var/lib/postgresql/15/main start"

sh -c "cd pgsql/bin; sudo -u postgres psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""

sh -c "sudo mkdir pgsql2"

sh -c "sudo cp -R pgsql pgsql2"

sh -c "sudo mkdir -p /var/lib/postgresql2/15/main"

sh -c "sudo chown -R postgres /var/lib/postgresql2/15/main"

sh -c "sudo chmod -R 777 /var/lib/postgresql2/15/main"

sh -c "cd pgsql2/bin; sudo -u postgres ./initdb -D /var/lib/postgresql2/15/main --auth trust --no-instructions"

sh -c "cd pgsql2/bin; sudo -u postgres ./pg_ctl -D /var/lib/postgresql2/15/main start"

sh -c "cd pgsql2/bin; sudo -u postgres ./psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""
