#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import pandas as pd

from iginx_pyclient.session import Session
from iginx_pyclient.thrift.rpc.ttypes import DataType, AggregateType


if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()

    # 获取集群拓扑信息
    cluster_info = session.get_cluster_info()
    print(cluster_info)

    # 写入数据
    paths = ["a.a.a", "a.a.b", "a.b.b", "a.c.c"]
    timestamps = [1, 2, 3, 4]
    values_list = [
        ['a', 'b', None, None],
        [None, None, 'b', None],
        [None, None, None, 'c'],
        ['Q', 'W', 'E', 'R'],
    ]
    data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
    session.insert_row_records(paths, timestamps, values_list, data_type_list)

    # 查询写入的数据
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

    # 使用 SQL 语句查询集群信息
    dataset = session.execute_statement("show cluster info;", fetch_size=2)

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
    dataset = session.execute_statement("show replica number;", fetch_size=2)

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

    # 使用 SQL 语句查询时间序列
    dataset = session.execute_statement("SHOW COLUMNS;", fetch_size=2)

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


    # 查询写入的数据
    dataset = session.query(["*"], 0, 10)
    print(dataset)


    # 统计每个序列的点数
    dataset = session.aggregate_query(["*"], 0, 10, AggregateType.COUNT)
    print(dataset)
    # 转换为pandas.Dataframe
    df_list = dataset.to_df()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    for df in df_list:
        print(df)
    """
       COUNT(count(a.a.a))  COUNT(count(a.a.b))  COUNT(count(a.b.b))  COUNT(count(a.c.c))
    0                    2                    2                    2                    2
    """

    # 获取部分序列的最后一个数据点
    dataset = session.last_query(["a.a.*"], 0)
    print(dataset)

    # 删除部分数据
    session.delete_time_series("a.b.b")

    # 查询删除后剩余的数据
    dataset = session.query(["*"], 0, 10)
    print(dataset)

    session.batch_delete_time_series(["*"])

    # 查询删除全部后剩余的数据
    dataset = session.query(["*"], 0, 10)
    print(dataset)

    session.close()
    print("关闭 session 成功")
