import sys, traceback
sys.path.append('session_py/')

from iginx.iginx_pyclient.session import Session
from iginx.iginx_pyclient.thrift.rpc.ttypes import StorageEngineType, StorageEngine, DataType, AggregateType, DebugInfoType

import time

if __name__ == '__main__':
    print("start")
    try:
        session = Session('127.0.0.1', 6888, "root", "root")
        session.open()
        # 输出所有存储引擎
        cluster_info = session.get_cluster_info()
        print(cluster_info)
        # add storage engine
        print("start adding storage engine")
        start_time = time.time()
        session.add_storage_engine(
            "127.0.0.1",
            5432,
            StorageEngineType.postgresql,
            {
                "has_data": "true",
                "is_read_only": "true",
                "username": "postgres",
                "password": "postgres"
            }
        )
        session.add_storage_engine(
            "127.0.0.1",
            27017,
            StorageEngineType.mongodb,
            {
                "has_data": "true",
                "is_read_only": "true",
                "schema.sample.size": "1000",
                "dummy.sample.size": "0"
            }
        )
        print(f"end adding storage engine, time cost: {time.time() - start_time}s")
        # 输出所有存储引擎
        cluster_info = session.get_cluster_info()
        print(cluster_info)
        ######################  test   #######################
        # 查询写入的数据，数据由PySessionIT测试写入
        dataset = self.session.query(["mongotpch.orders.*"], 0, 5)
        # 转换为pandas.Dataframe
        df = dataset.to_df()
        print(df)
        # 查询写入的数据，数据由PySessionIT测试写入
        dataset = self.session.query(["postgres.customer.*"], 0, 5)
        # 转换为pandas.Dataframe
        df = dataset.to_df()
        print(df)
        # 查询写入的数据，数据由PySessionIT测试写入
        dataset = self.session.query(["nation.*"], 0, 5)
        # 转换为pandas.Dataframe
        df = dataset.to_df()
        print(df)
        ###################  end  test  #######################
        # 开始tpch查询
        print("start tpch query")
        start_time = time.time()

        print(f"end tpch query, time cost: {time.time() - start_time}s")
        session.close()
    except Exception as e:
        print(e)
        traceback.print_exc()

