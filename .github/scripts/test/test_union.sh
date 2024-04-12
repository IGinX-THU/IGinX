#!/bin/bash

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
     echo " test  -- Success !"
     echo " test  -- Success !" >> ./test/src/test/resources/testResult.txt
   fi
done

cd test