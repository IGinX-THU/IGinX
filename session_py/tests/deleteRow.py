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

        # 这里因为key=1的这一行只有a.b.b有值，所以删除a.b.b这一列后这一整行数据就被删除了
        session.delete_data("a.b.b", 1, 2)

        # 查询删除后剩余的数据
        dataset = session.query(["*"], 0, 10)
        print(dataset)

        # 删除部分数据（设置为null
        session.batch_delete_data(["a.a.a", "a.a.b"], 2, 4)
        # 查询删除后剩余的数据
        dataset = session.query(["*"], 0, 10)
        print(dataset)

        session.close()
    except Exception as e:
        print(e)
        if e == ("Error occurs: Unable to delete data from read-only nodes. The data of the writable nodes has been "
                 "cleared."):
            exit(0)
        exit(1)