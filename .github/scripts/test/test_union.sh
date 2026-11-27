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
 
python -VV
pwd

cd ..

cat ./test/src/test/resources/testTask.txt

i=0

while read line
do
  LISTS[$i]=$line
  let i+=1
done <  ./test/src/test/resources/testTask.txt

echo "test IT name list : "${LISTS[*]}

echo "test IT name list : "${LISTS[*]} > ./test/src/test/resources/testResult.txt

for line in ${LISTS[@]}
do
   echo "test IT name : "$line
   echo "test IT name : "$line >> ./test/src/test/resources/testResult.txt
   mvn test -q -Dtest=$line -DfailIfNoTests=false  -P-format

   if [ $? -ne 0 ];then
     echo " test  -- Faile  : "$?
     echo " test  -- Faile  : "$? >> ./test/src/test/resources/testResult.txt
     exit 1
   else
     if [ "$line" != "LegacyBehaviorIT" ] || [ "${IGINX_TEST_SECURITY:-false}" != "true" ]; then
       for report in test/target/surefire-reports/TEST-*."$line".xml; do
         python .github/scripts/test/assert_test_reports.py "$report:1" || exit 1
       done
     fi
     echo " test  -- Success !"
     echo " test  -- Success !" >> ./test/src/test/resources/testResult.txt
   fi
done

cd test
