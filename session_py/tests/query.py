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

from iginx.session import Session

if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()

    # 获取集群拓扑信息
    cluster_info = session.get_cluster_info()
    print(cluster_info)


    # 查询写入的数据，数据由PySessionIT测试写入
    dataset = session.query(["a.*"], 0, 10)
    print(dataset)
    # 转换为pandas.Dataframe
    df = dataset.to_df()
    print(df)
    """
       key a.a.a a.a.b a.b.b a.c.c
    0    1  b'a'  b'b'  None  None
    1    2  None  None  b'b'  None
    2    3  None  None  None  b'c'
    3    4  b'Q'  b'W'  b'E'  b'R'
    """
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

    # 使用 SQL 语句查询副本数量
    replicaNum = session.get_replica_num()

    print('replicaNum:', replicaNum)

    dataset.close()

    session.close()
