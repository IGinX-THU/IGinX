#!/bin/bash

set -e

cp -f test/src/test/resources/udf/mock_udf.py client/target/iginx-client-0.6.0-SNAPSHOT/sbin/mock_udf.py

ls client/target/iginx-client-0.6.0-SNAPSHOT/sbin

COMMAND='REGISTER UDAF PYTHON TASK "'"MockUDF"'" IN "'"mock_udf.py"'" AS "'"mock_udf"'";'

cd client/target/iginx-client-0.6.0-SNAPSHOT/sbin

bash -c "start_cli.bat -e '$COMMAND'"
