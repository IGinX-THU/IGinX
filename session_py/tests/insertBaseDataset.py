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
import traceback


if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()
    try:
        # 查询写入之前的数据
        dataset = session.query(["test.*"], 0, 10)
        print('Before insert: ', dataset)
        # df = dataset.to_df()
        # # 检查df是否如下：
        # """
        #        key a.a.a a.a.b a.b.b a.c.c
        #     0    1  b'a'  b'b'  None  None
        #     1    2  None  None  b'b'  None
        #     2    3  None  None  None  b'c'
        #     3    4  b'Q'  b'W'  b'E'  b'R'
        # """
        # if(df.size != 0):
        #     print('Before insert: ', df)
        #     exit(0)

        # 写入数据
        paths = ["test.a.a", "test.a.b", "test.b.b", "test.c.c"]
        timestamps = [0, 1, 2, 3]
        values_list = [
            ['a', 'b', None, None],
            [None, None,'b', None],
            [None, None, None, 'c'],
            ['Q', 'W', 'E', 'R']
            # ['a', None, 'c', 'd'],
            # ['e', 'f', None, 'h'],
            # ['i', '', 'k', None],
            # ['Q', 'W', 'E', 'R']
        ]
        data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
        session.insert_row_records(paths, timestamps, values_list, data_type_list)
        # 查询写入的数据
        dataset = session.query(["test.*"], 0, 10)
        print(dataset)

    except Exception as e:
        traceback.print_exc()
        if str(e) == 'Error occurs: The query results contain overlapped keys.':
            exit(0)
        print(e)
        exit(1)
    finally:
        session.close()