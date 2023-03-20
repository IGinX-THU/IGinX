#!/bin/sh

set -e

sh -c "chmod +x .github/iotdb12_macos.sh"

sh -c "chmod +x .github/influxdb_macos.sh"

sh -c ".github/iotdb12_macos.sh"

sh -c ".github/influxdb_macos.sh"

set -i "s/storageEngineList/#storageEngineList/g" conf/config.properties

echo "storageEngineList=127.0.0.1#6667#iotdb12#username=root#password=root#sessionPoolSize=50#has_data=false#is_read_only=false,127.0.0.1#8086#influxdb#url=http://localhost:8086/#token=testToken#organization=testOrg#has_data=false" >> conf/config.properties
