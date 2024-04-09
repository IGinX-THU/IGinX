# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# 无法单独执行，用来测试PySessionIT
import sys
sys.path.append('../session_py/')  # 将上一级目录添加到Python模块搜索路径中

from iginx.iginx_pyclient.session import Session
from iginx.iginx_pyclient.thrift.rpc.ttypes import DataType


if __name__ == '__main__':
    try:
        session = Session('127.0.0.1', 6888, "root", "root")
        session.open()

        # 写入数据
        paths = ["test.a.a", "test.a.b", "test.b.b", "test.c.c"]
        timestamps = [5, 6, 7]
        values_list = [
            [None, None, 'a', 'b'],
            ['b', None, None, None],
            ['R', 'E', 'W', 'Q']
        ]
        data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
        session.insert_row_records(paths, timestamps, values_list, data_type_list)
        # 查询写入的数据
        dataset = session.query(["test.*"], 0, 10)
        print(dataset)

        paths = ["test.a.a", "test.a.b", "test.b.b"]
        timestamps = [8, 9]
        values_list = [
            [None, 'a', 'b'],
            ['b', None, None]
        ]
        data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY]
        session.insert_non_aligned_row_records(paths, timestamps, values_list, data_type_list)
        # 查询写入的数据
        dataset = session.query(["test.*"], 0, 10)
        print(dataset)

        # 插入列
        paths = ["test.b.c"]
        timestamps = [6]
        values_list = [[1]]
        data_type_list = [DataType.INTEGER]
        session.insert_column_records(paths, timestamps, values_list, data_type_list)
        # 查询写入的数据
        dataset = session.query(["test.*"], 0, 10)
        print(dataset)

        # 插入列
        paths = ["test.b.c"]
        timestamps = [5]
        values_list = [[1]]
        data_type_list = [DataType.INTEGER]
        session.insert_non_aligned_column_records(paths, timestamps, values_list, data_type_list)
        # 查询写入的数据
        dataset = session.query(["test.*"], 0, 10)
        print(dataset)

        session.close()
    except Exception as e:
        print(e)
        exit(1)