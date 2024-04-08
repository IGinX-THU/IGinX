#!/bin/bash

set -e

cp -f test/src/test/resources/udf/mock_udf.py client/target/iginx-client-0.6.0-SNAPSHOT/sbin/mock_udf.py

ls client/target/iginx-client-0.6.0-SNAPSHOT/sbin

COMMAND='CREATE FUNCTION UDAF "'"MockUDF"'" FROM "'"mock_udf"'" IN "'"mock_udf.py"'";'

cd client/target/iginx-client-0.6.0-SNAPSHOT/sbin

result=$(bash -c "./start_cli.bat -e '$COMMAND'")

if [[ $result =~ 'success' ]]; then
  echo success
  COMMAND='DROP PYTHON TASK "'"mock_udf"'";'
  bash -c "./start_cli.bat -e '$COMMAND'"
else
  echo 'Error: failed to register udf mock_udf.'
  echo $result
  exit 1
fi
