#!/bin/sh

set -e

cp -f test/src/test/resources/udf/mock_udf.py client/target/iginx-client-0.6.0-SNAPSHOT/sbin/mock_udf.py

ls client/target/iginx-client-0.6.0-SNAPSHOT/sbin

COMMAND='CREATE FUNCTION UDAF "'"mock_udf"'" FROM "'"MockUDF"'" IN "'"mock_udf.py"'";'

cd client/target/iginx-client-0.6.0-SNAPSHOT/sbin

sh -c "chmod +x start_cli.sh"

result=$(sh -c "echo '$COMMAND' | xargs -0 -t -I F sh start_cli.sh -e 'F'")

if [[ $result =~ 'success' ]]; then
  echo success
  COMMAND='DROP FUNCTION "'"mock_udf"'";'
  sh -c "echo '$COMMAND' | xargs -0 -t -I F sh start_cli.sh -e 'F'"
else
  echo 'Error: failed to register udf mock_udf.'
  echo $result
  exit 1
fi

