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


class LoadDirectory:
    def __init__(self):
        pass

    def test(self):
        retStr = ""
        try:
            session = Session('127.0.0.1', 6888, "root", "root")
            session.open()

            # calculate file path
            import os
            path = os.getcwd() + '/../session_py/tests/dir'
            # path = 'dir/'
            session.load_directory(path)

            # 使用 SQL 语句查询写入的数据
            dataset = session.execute_statement("select * from dir;", fetch_size=2)

            columns = dataset.columns()
            for column in columns:
                retStr += column + "\t"
            retStr += "\n"

            while dataset.has_more():
                row = dataset.next()
                for field in row:
                    retStr += str(field) + "\t\t"
                retStr += "\n"
            retStr += "\n"
            dataset.close()
            session.close()
            return retStr
        except Exception as e:
            print(e)
            exit(1)
