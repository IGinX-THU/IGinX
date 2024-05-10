import sys, traceback


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

        # sql = 'select * from mongotpch.orders limit 5;'
        sql = '''select 
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
            postgres.region.r_name = "ASIA"
            and mongotpch.orders.o_orderdate >= 757353600000
            and mongotpch.orders.o_orderdate < 788889600000
    )
    group by
        nation.n_name
)
order by
    revenue desc;''' # 这里757353600000和788889600000分别是1994-01-01和1995-01-01的Unix时间戳
        # 执行查询语句
        dataset = session.execute_statement(sql)
        columns = dataset.columns()
        for column in columns:
            print(str(column) + '    ', end='')
        print()

        result = []
        while dataset.has_more():
            row = dataset.next()
            rowResult = []
            for field in row:
                print(str(field) + '        ', end='')
                rowResult.append(str(field))
            result.append(rowResult)
            print()
        print()
        print(result)
        dataset.close()
        print(f"end tpch query, time cost: {time.time() - start_time}s")

        # 正确性验证 读取csv文件中的正确结果
        line_count = -1 # 用于跳过第一行
        correct = True
        with open('test/src/test/resources/polybench/sf0.1/q05.csv', 'r') as f:
            for line in f:
                if line.strip() == '':
                    break
                if line_count < 0:
                    line_count += 1
                    continue
                answer = line.strip().split('|')
                print(f"line count: {line_count}, answer: {answer}")
                print(f"result: {eval(result[line_count][0]).decode('utf-8')}, answer: {answer[0]}")
                if eval(result[line_count][0]).decode('utf-8') != answer[0]:
                    correct = False
                    break
                print(f"result: {eval(result[line_count][1])}, answer: {answer[1]}")
                if abs(eval(result[line_count][1]) - eval(answer[1])) > 0.1:
                    correct = False
                    break
                line_count += 1
        if not correct:
            print("incorrect result")
            exit(1)

        # 重复测试五次
        execute_time = []
        for i in range(5):
            start_time = time.time()
            dataset = session.execute_statement(sql)
            execute_time.append(time.time() - start_time)
            print(f"tpch query{i}, time cost: {execute_time[i]}s")

        # 输出平均执行时间
        print(f"average time cost: {sum(execute_time) / 5}s")

        session.close()
    except Exception as e:
        print(e)
        traceback.print_exc()

