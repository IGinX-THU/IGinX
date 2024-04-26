import sys, traceback
sys.path.append('../session_py/')  # 将上一级目录添加到Python模块搜索路径中

from iginx.iginx_pyclient.session import Session
from iginx.iginx_pyclient.thrift.rpc.ttypes import StorageEngineType, StorageEngine, DataType, AggregateType, DebugInfoType

import time

if __name__ == '__main__':
    print("start")
    try:
        session = Session('127.0.0.1', 6888, "root", "root")
        session.open()
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
        print("end adding storage engine")
        # 输出所有存储引擎
        cluster_info = session.get_cluster_info()
        print(cluster_info)
        session.close()
    except Exception as e:
        print(e)
        traceback.print_exc()

