#!/bin/bash

chmod +x .github/scripts/test/SDMF/*.sh

.github/scripts/test/SDMF/SDMF_insert.sh

sleep 10

.github/scripts/test/SDMF/SDMF_query.sh

sleep 10

.github/scripts/test/SDMF/result_merge.sh