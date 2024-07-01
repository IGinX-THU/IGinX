#!/bin/bash

# 读取 tbl 文件
input_file="../IGinX/tpc/TPC-H V3.0.1/data/nation.tbl"
output_file="../IGinX/tpc/TPC-H V3.0.1/data/nation.csv"

# 如果文件已存在，则删除
if [ -f "$output_file" ]; then
    rm "$output_file"
fi

# 处理每一行数据
line_number=1
while IFS='|' read -r fields; do
    # 将 "|" 分隔符替换为 ","
    fields=$(echo "$fields" | tr '|' ',')
    # 删除每行末尾的逗号
    fields=$(echo "$fields" | sed 's/,$//')
    # 添加行号并将处理后的数据写入 CSV 文件
    echo "$line_number,$fields" >> "$output_file"
    ((line_number++))
done < "$input_file"

cat "$output_file"

echo "Conversion completed. CSV file: $output_file"

cat "$output_file"

# 插入数据
COMMAND1='LOAD DATA FROM INFILE "tpc/TPC-H V3.0.1/data/nation.csv" AS CSV INTO nation(key, n_nationkey, n_name, n_regionkey, n_comment);CREATE FUNCTION UDTF "extractYear" FROM "UDFExtractYear" IN "test/src/test/resources/polybench/udf/udtf_extract_year.py";'
SCRIPT_COMMAND="bash client/target/iginx-client-0.7.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "chmod +x client/target/iginx-client-0.7.0-SNAPSHOT/sbin/start_cli.sh"

if [ "$RUNNER_OS" = "Linux" ]; then
  bash -c "echo '$COMMAND1' | xargs -0 -t -i ${SCRIPT_COMMAND}"
elif [ "$RUNNER_OS" = "Windows" ]; then
  bash -c "client/target/iginx-client-0.7.0-SNAPSHOT/sbin/start_cli.bat -e '$COMMAND1'"
elif [ "$RUNNER_OS" = "macOS" ]; then
  sh -c "echo '$COMMAND1' | xargs -0 -t -I F sh client/target/iginx-client-0.7.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"
fi
