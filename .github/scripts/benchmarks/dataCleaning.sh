#!/bin/bash

set -e

COMMAND1='LOAD DATA FROM INFILE "dataCleaning/zipcode_city.csv" AS CSV INTO uszip(key,city,zipcode);SELECT count(a.zipcode) FROM uszip as a JOIN uszip as b ON a.zipcode = b.zipcode WHERE a.city <> b.city;'

SCRIPT_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

if [ "$RUNNER_OS" = "Linux" ]; then
  bash -c "echo '$COMMAND1' | xargs -0 -t -i ${SCRIPT_COMMAND}"
elif [ "$RUNNER_OS" = "Windows" ]; then
  bash -c "client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.bat -e '$COMMAND1'"
elif [ "$RUNNER_OS" = "macOS" ]; then
  sh -c "echo '$COMMAND1' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"
fi