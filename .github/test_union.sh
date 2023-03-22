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

for line in ${LISTS[@]}
do
   echo "test IT name : "$line
   mvn test -q -Dtest=$line -DfailIfNoTests=false

   if [ $? -ne 0 ];then
     echo " test  -- Faile  : "$?
     exit 1
   else
     echo " test " $line " Success !"
   fi
done

cd test