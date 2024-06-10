#!/bin/bash

# 检查read_output_all.txt文件是否包含"exception"字段
if grep -q "Exception" "read_output_all.txt"; then
    echo "Error: Exception found in read_output_all.txt."
#    exit 1
else
    echo "No exception found in read_output_all.txt."
#    exit 0
fi