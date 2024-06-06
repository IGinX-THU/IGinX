#!/bin/bash

SCRIPT_COMMAND="/home/lhz/lhz/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"
TIMES=$1
PLL=10
echo "" > read_output.txt
mkdir tmp
for ((k = 1; k <= $TIMES; k++)); do
    for ((i = 1; i <= $PLL; i += 1)); do
        COMMAND="select a1,a2,a3,a4,a5,a6,a7,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19 from a.b where key<100000;"
        TEMP_FILE=./tmp/tmp_${k}_${i}.txt
        echo "$COMMAND" > "$TEMP_FILE"
        # 使用重定向输入的方式执行命令
        # bash -c "cat $TEMP_FILE | $SCRIPT_COMMAND  >> ./tmp/read_output_${k}_${i}.txt && cat ./tmp/read_output_${k}_${i}.txt >> read_output.txt &" &
        bash -c "cat $TEMP_FILE | $SCRIPT_COMMAND  >> ./tmp/read_output_${k}_${i}.txt &" &
    done
done

for ((k = 1; k <= $TIMES; k++)); do
    for ((i = 1; i <= $PLL; i += 1)); do
        rm ./tmp/tmp_${k}_${i}.txt
    done
done
echo "query done"



