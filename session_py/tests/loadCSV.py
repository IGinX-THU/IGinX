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


if __name__ == '__main__':
    try:
        session = Session('127.0.0.1', 6888, "root", "root")
        session.open()

        # calculate file path
        import os
        # print(os.getcwd())
        # 这里在用junit test运行时，对应的路径为： Iginx/test
        path = os.getcwd() + '/../session_py/tests/files/a.csv'
        statement = f"LOAD DATA FROM INFILE '{path}' AS CSV INTO a(key, a.a, a.b, b.b, c.c);"


        # 使用 SQL 语句查询写入的数据
        dataset = session.execute_statement("select * from a;", fetch_size=2)

        columns = dataset.columns()
        for column in columns:
            print(column, end="\t")
        print()

        while dataset.has_more():
            row = dataset.next()
            for field in row:
                print(str(field), end="\t\t")
            print()
        print()

        dataset.close()

        session.close()
    except Exception as e:
        print(e)
        exit(1)
