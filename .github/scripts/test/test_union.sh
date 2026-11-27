#!/bin/bash
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
 
set -u
python -VV
cd .. || exit 1
tests=()
while IFS= read -r line || [ -n "$line" ]; do tests+=("$line"); done < ./test/src/test/resources/testTask.txt
result=./test/src/test/resources/testResult.txt
printf 'Tests: %s\n' "${tests[*]}" > "$result"
failed=0
for test in "${tests[@]}"; do
  test=${test%$'\r'}
  [ -z "$test" ] && continue
  printf 'Starting %s\n' "$test" | tee -a "$result"
  # Each class and the surrounding Controller have independent deadlines.
  python .github/scripts/test/run_bounded.py --timeout "${IGINX_TEST_CLASS_TIMEOUT:-900}" --label "$test" --retry-downloads 2 -- \
    mvn test -q "-Dtest=$test" -DfailIfNoTests=false -P-format
  code=$?
  if [ "$code" -eq 0 ] && { [ "$test" != "LegacyBehaviorIT" ] || [ "${IGINX_TEST_SECURITY:-false}" != "true" ]; }; then
    for report in test/target/surefire-reports/TEST-*."$test".xml; do
      python .github/scripts/test/assert_test_reports.py "$report:1" || code=1
    done
  fi
  printf '%s exit=%s\n' "$test" "$code" | tee -a "$result"
  # Retain all failures and run the remaining classes, rather than hiding them
  # behind the first assertion failure. The outer deadline still bounds the suite.
  [ "$code" -ne 0 ] && failed=1
done
exit "$failed"
