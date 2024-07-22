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


if [ "$RUNNER_OS" = "Windows" ]; then
  python test/src/test/resources/tpch/thu_cloud_download.py \
    -l https://cloud.tsinghua.edu.cn/d/740c158819bc4759a36e/ \
    -s  "."
else
  python3 test/src/test/resources/tpch/thu_cloud_download.py \
    -l https://cloud.tsinghua.edu.cn/d/740c158819bc4759a36e/ \
    -s  "."
fi
  cd tpchdata
  # 目标文件夹路径
  destination_folder="../tpc/TPC-H V3.0.1/data"

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
