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
from iginx.iginx_pyclient.thrift.rpc.ttypes import StorageEngineType, StorageEngine

if __name__ == '__main__':
    try:
        session = Session('127.0.0.1', 6888, "root", "root")
        session.open()
        cluster_info = session.get_cluster_info()
        original_cluster_info = cluster_info.get_storage_engine_list()
        for storage_engine in original_cluster_info:
            if storage_engine.port == 5432 or storage_engine.port == 27017:
                print("This engine is already in the cluster.")
                exit(0)
        session.add_storage_engine(
            "127.0.0.1",
            5432,
            StorageEngineType.postgresql,
            {
                "username": "postgres",
                "password": "postgres",
                "has_data": "true",
                "is_read_only": "true"
            }
        )
        # 输出所有存储引擎
        cluster_info = session.get_cluster_info()
        print(cluster_info)
        # 删除加入的存储引擎
        session.execute_sql('REMOVE HISTORYDATASOURCE  ("127.0.0.1", 5432, "", "");')
        # 删除后输出所有存储引擎
        cluster_info = session.get_cluster_info()
        print(cluster_info)
        # 批量加入存储引擎
        pg_engine = StorageEngine(
            "127.0.0.1",
            5432,
            StorageEngineType.postgresql,
            {
                "username": "postgres",
                "password": "postgres",
                "has_data": "true",
                "is_read_only": "true"
            }
        )
        mongo_engine = StorageEngine(
            "127.0.0.1",
            27017,
            StorageEngineType.mongodb,
            {
                "has_data": "true",
                "is_read_only": "true"
            }
        )
        session.batch_add_storage_engine([pg_engine, mongo_engine])
        # 输出所有存储引擎
        cluster_info = session.get_cluster_info()
        print(cluster_info)
        # 删除加入的存储引擎
        session.execute_sql('REMOVE HISTORYDATASOURCE  ("127.0.0.1", 5432, "", "");')
        session.execute_sql('REMOVE HISTORYDATASOURCE  ("127.0.0.1", 27017, "", "");')
        # 删除后输出所有存储引擎
        cluster_info = session.get_cluster_info()
        print(cluster_info)

        session.close()
    except Exception as e:
        print(e)
        exit(1)
