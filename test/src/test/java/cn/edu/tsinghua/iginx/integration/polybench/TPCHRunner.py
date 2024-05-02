import sys, traceback, signal
sys.path.append('session_py/')

from iginx.iginx_pyclient.session import Session
from iginx.iginx_pyclient.thrift.rpc.ttypes import StorageEngineType, StorageEngine, DataType, AggregateType, DebugInfoType

import time

# 定义信号处理函数
def timeout_handler(signum, frame):
    print("Execution time exceeded 5 minutes. Exiting...")
    sys.exit(0)

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
        dataset = session.query(["mongotpch.orders.*"], 0, 5)
        # 转换为pandas.Dataframe
        df = dataset.to_df()
        print(df)
        # 查询写入的数据，数据由PySessionIT测试写入
        dataset = session.query(["postgres.customer.*"], 0, 5)
        # 转换为pandas.Dataframe
        df = dataset.to_df()
        print(df)
        # 查询写入的数据，数据由PySessionIT测试写入
        dataset = session.query(["nation.*"], 0, 5)
        # 转换为pandas.Dataframe
        df = dataset.to_df()
        print(df)
        # 使用 list_time_series() 接口查询时间序列
        timeSeries = session.list_time_series()
        for ts in timeSeries:
            print(ts)
        ###################  end  test  #######################
        # 开始tpch查询
        print("start tpch query")
        start_time = time.time()

        sql = 'select * from mongotpch.orders limit 5;'
        '''select 
    nation.n_name,
    revenue
from (
    select
        nation.n_name,
        sum(tmp) as revenue
    from (
        select
            nation.n_name,
            mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) as tmp
        from
            postgres.customer
            join mongotpch.orders on postgres.customer.c_custkey = mongotpch.orders.o_custkey
            join mongotpch.lineitem on mongotpch.lineitem.l_orderkey = mongotpch.orders.o_orderkey
            join postgres.supplier on mongotpch.lineitem.l_suppkey = postgres.supplier.s_suppkey and postgres.customer.c_nationkey = postgres.supplier.s_nationkey
            join nation on postgres.supplier.s_nationkey = nation.n_nationkey
            join postgres.region on nation.n_regionkey = postgres.region.r_regionkey
        where
            postgres.region.r_name = "EUROPE"
    )
    group by
        nation.n_name
)
order by
    revenue desc;'''
        # 设置 SIGALRM 信号处理器
        signal.signal(signal.SIGALRM, timeout_handler)
        # 设置定时器，5分钟后触发 SIGALRM 信号
        signal.alarm(300)  # 300 秒 = 5 分钟
        # 执行查询语句
        dataset = session.execute_statement(sql)
        # 取消定时器
        signal.alarm(0)
        print(f"end tpch query, time cost: {time.time() - start_time}s")
        # 获取执行语句后的内存使用情况
        memory_usage = get_memory_usage()
        print("Memory usage:", memory_usage, "MB")
        session.close()
    except Exception as e:
        print(e)
        traceback.print_exc()

