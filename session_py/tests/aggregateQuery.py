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
sys.path.append('../session_py')  # 将上一级目录添加到Python模块搜索路径中

from iginx.iginx_pyclient.session import Session
from iginx.iginx_pyclient.thrift.rpc.ttypes import AggregateType
import pandas as pd

if __name__ == '__main__':
    try:
        session = Session('127.0.0.1', 6888, "root", "root")
        session.open()

        # 统计每个序列的点数
        dataset = session.aggregate_query(["*"], 0, 10, AggregateType.COUNT)
        print(dataset)
        # 转换为pandas.Dataframe
        df_list = dataset.to_df()
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        for df in df_list:
            print(df)

        session.close()
    except Exception as e:
        print(e)
        exit(1)
