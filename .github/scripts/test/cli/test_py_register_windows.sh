#!/bin/bash

set -e

cp -f test/src/test/resources/udf/mock_udf.py client/target/iginx-client-$1/sbin/mock_udf.py

ls client/target/iginx-client-$1/sbin

COMMAND='CREATE FUNCTION UDAF "'"mock_udf"'" FROM "'"MockUDF"'" IN "'"mock_udf.py"'";'

cd client/target/iginx-client-$1/sbin

result=$(bash -c "./start_cli.bat -e '$COMMAND'")

if [[ $result =~ 'success' ]]; then
  echo success
  COMMAND='DROP FUNCTION "'"mock_udf"'";'
  bash -c "./start_cli.bat -e '$COMMAND'"
else
  echo 'Error: failed to register udf mock_udf.'
  echo $result
  exit 1
fi
