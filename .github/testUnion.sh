#!/bin/bash

pwd

cd ..

i=0

while read line
do
  LISTS[$i]=$line
  let i+=1
done <  ./test/src/test/java/cn/edu/tsinghua/iginx/integration/testcontroler/testTask.txt

echo "test IT name list : "${LISTS[*]}

for line in ${LISTS[@]}
do
   echo "test IT name : "$line
   mvn test -q -Dtest=$line -DfailIfNoTests=false

   if [ $? -ne 0 ];then
     echo " test  -- Faile  : "$?
     exit 1
   else
     echo " test  -- Success !"
   fi
done

cd test