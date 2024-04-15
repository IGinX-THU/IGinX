#!/bin/bash

set -e

cp -f test/src/test/resources/udf/mock_udf.py client/target/iginx-client-0.6.0-SNAPSHOT/sbin/mock_udf.py

ls client/target/iginx-client-0.6.0-SNAPSHOT/sbin

COMMAND='CREATE FUNCTION UDAF "'"mock_udf"'" FROM "'"MockUDF"'" IN "'"mock_udf.py"'";'

cd client/target/iginx-client-0.6.0-SNAPSHOT/sbin

SCRIPT_COMMAND="bash start_cli.sh -e '{}'"

bash -c "chmod +x start_cli.sh"

result=$(bash -c "echo '$COMMAND' | xargs -0 -t -i ${SCRIPT_COMMAND}")

if [[ $result =~ 'success' ]]; then
  echo success
  COMMAND='DROP PYTHON TASK "'"mock_udf"'";'
  bash -c "echo '$COMMAND' | xargs -0 -t -i ${SCRIPT_COMMAND}"
else
  echo 'Error: failed to register udf mock_udf.'
  echo $result
  exit 1
fi
