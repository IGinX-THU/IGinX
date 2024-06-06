#!/bin/bash
TIMES=10
PLL=10
echo "" > read_output_all.txt
# 将单独的输出文件追加到总的输出文件中
for ((k = 1; k <= $TIMES; k++)); do
    for ((i = 0; i <= $PLL; i += 1)); do
        OUTPUT_FILE="./tmp/read_output_${k}_${i}.txt"
        cat "$OUTPUT_FILE" >> read_output_all.txt
        rm "$OUTPUT_FILE"  # 清理单独的输出文件
    done
done
