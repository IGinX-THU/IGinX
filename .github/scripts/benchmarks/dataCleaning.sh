#!/bin/bash

python3 dataCleaning/gen_data.py -n 1000000
set -e

COMMAND1='LOAD DATA FROM INFILE "../IGinX/dataCleaning/zipcode_city.csv" AS CSV INTO uszip(key,city,zipcode);SELECT count(a.zipcode) FROM uszip as a JOIN uszip as b ON a.zipcode = b.zipcode WHERE a.city <> b.city;'

SCRIPT_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

bash -c "echo '$COMMAND1' | xargs -0 -t -i ${SCRIPT_COMMAND}"