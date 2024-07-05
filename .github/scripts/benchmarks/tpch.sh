#!/bin/bash

if [ "$RUNNER_OS" = "Windows" ]; then
  python thu_cloud_download.py \
    -l https://cloud.tsinghua.edu.cn/d/740c158819bc4759a36e/ \
    -s  "."
else
  python3 thu_cloud_download.py \
    -l https://cloud.tsinghua.edu.cn/d/740c158819bc4759a36e/ \
    -s  "."
fi
  cd tpchdata
  # 目标文件夹路径
  destination_folder="../../tpc/TPC-H V3.0.1/data"

  # 确保目标文件夹存在，如果不存在则创建
  mkdir -p "$destination_folder"

  # 将所有*.tbl文件移动到目标文件夹
  mv *.tbl "$destination_folder/"
  cd "$destination_folder"

  chmod +r customer.tbl
  chmod +r lineitem.tbl
  chmod +r nation.tbl
  chmod +r orders.tbl
  chmod +r region.tbl
  chmod +r supplier.tbl
  ls -a
  pwd
  echo "文件移动完成"
