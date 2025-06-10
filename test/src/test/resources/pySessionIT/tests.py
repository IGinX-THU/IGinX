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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# 无法单独执行，用来测试PySessionIT
import sys, traceback
sys.path.append('../session_py/')  # 将上一级目录添加到Python模块搜索路径中

from iginx.iginx_pyclient.session import Session
from iginx.iginx_pyclient.thrift.rpc.ttypes import StorageEngineType, StorageEngine, DataType, AggregateType, DebugInfoType

class Tests:
    def __init__(self):
        self.session = Session('127.0.0.1', 6888, "root", "root")
        self.session.open()
        pass

    def __del__(self):
        self.session.close()
        pass

    def addStorageEngine(self):
        retStr = ""
        try:
            import os
            os.makedirs('pq/data', mode=0o777, exist_ok=True)
            os.makedirs('pq/dummy', mode=0o777, exist_ok=True)
            import pandas as pd
            # 创建一个示例数据框
            data = pd.DataFrame({
                'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Emily'],
                'Age': [25, 30, 35, 40, 45],
                'Salary': [50000, 60000, 70000, 80000, 90000]
            })
            # 将数据框保存为 Parquet 文件
            data.to_parquet('pq/dummy/example.parquet', index=False)
            print("Dummy Parquet 文件已生成：example.parquet")

            os.makedirs('fs/data', mode=0o777, exist_ok=True)
            os.makedirs('fs/dummy', mode=0o777, exist_ok=True)
            cluster_info = self.session.get_cluster_info()
            if "port=6670" in str(cluster_info) or "port=6671" in str(cluster_info):
                retStr = "The storage engine has been added, please delete it first\n"
                return retStr
            self.session.add_storage_engine(
                "127.0.0.1",
                6670,
                StorageEngineType.filesystem,
                {
                    "dummy_dir": f"{os.getcwd()}/pq/dummy",
                    "iginx_port": "6888",
                    "has_data": "true",
                    "is_read_only": "true",
                }
            )
            # 输出所有存储引擎
            cluster_info = self.session.get_cluster_info()
            retStr += str(cluster_info) + "\n"
            # 删除加入的存储引擎
            self.session.execute_sql('REMOVE STORAGEENGINE ("127.0.0.1", 6670, "", "") FOR ALL;')
            # 删除后输出所有存储引擎
            cluster_info = self.session.get_cluster_info()
            retStr += str(cluster_info) + "\n"
            # 批量加入存储引擎
            pq_engine = StorageEngine(
                "127.0.0.1",
                6670,
                StorageEngineType.filesystem,
                {
                    "dummy_dir": f"{os.getcwd()}/pq/dummy",
                    "iginx_port": "6888",
                    "has_data": "true",
                    "is_read_only": "true",
                }
            )
            fs_engine = StorageEngine(
                "127.0.0.1",
                6671,
                StorageEngineType.filesystem,
                {
                    "dummy_dir": f"{os.getcwd()}/fs/dummy",
                    "iginx_port": "6888",
                    "has_data": "true",
                    "is_read_only": "true",
                }
            )
            self.session.batch_add_storage_engine([pq_engine, fs_engine])
            # 输出所有存储引擎
            cluster_info = self.session.get_cluster_info()
            retStr += str(cluster_info) + "\n"
            # 删除加入的存储引擎
            self.session.execute_sql('REMOVE STORAGEENGINE ("127.0.0.1", 6670, "", "") FOR ALL;')
            self.session.execute_sql('REMOVE STORAGEENGINE ("127.0.0.1", 6671, "", "") FOR ALL;')
            # 删除新建的parquet文件
            os.remove('pq/dummy/example.parquet')
            # 删除新建的文件夹
            os.rmdir('pq/data')
            os.rmdir('pq/dummy')
            os.rmdir('pq')
            os.rmdir('fs/data')
            os.rmdir('fs/dummy')
            os.rmdir('fs')
            # 删除后输出所有存储引擎
            cluster_info = self.session.get_cluster_info()
            retStr += str(cluster_info) + "\n"

            return retStr
        except Exception as e:
            traceback.print_exc()
            print(e)
            retStr += str(e) + "\n"
            exit(1)

    def aggregateQuery(self):
        retStr = ""
        try:
            # 统计每个序列的点数
            # 设置显示所有列
            import pandas as pd
            pd.set_option('display.max_columns', None)
            dataset = self.session.aggregate_query(["test.*"], 0, 10, AggregateType.COUNT)
            retStr += str(dataset.to_df()) + "\n"

            return retStr
        except Exception as e:
            print(e)
            exit(1)
    def deleteAll(self):
        try:
            self.session.batch_delete_time_series(["*"])
        except Exception as e:
            if str(e) == (
            "Error occurs: Unable to delete data from read-only nodes. The data of the writable nodes has "
            "been cleared."):
                exit(0)
            traceback.print_exc()
            print(e)
            exit(1)
        finally:
            # 查询删除全部后剩余的数据
            try:
                dataset = self.session.query(["test.*"], 0, 10)
                print(dataset)
            except Exception as e:
                traceback.print_exc()
                print(e)
                exit(1)
            finally:

                return ""

    def deleteColumn(self):
        retStr = ""
        try:
            # 删除部分数据
            # 写入数据
            paths = ["test.a.a", "test.a.b", "test.b.b", "test.c.c"]
            timestamps = [5, 6, 7]
            values_list = [
                [None, None, 'a', 'b'],
                ['b', None, None, None],
                ['R', 'E', 'W', 'Q']
            ]
            data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
            self.session.insert_row_records(paths, timestamps, values_list, data_type_list)
            self.session.delete_time_series("test.b.b")
        except Exception as e:
            if str(e) == (
            "Error occurs: Unable to delete data from read-only nodes. The data of the writable nodes has "
            "been cleared."):
                exit(0)
            print(e)
            exit(1)
        finally:
            # 查询删除后剩余的数据
            dataset = self.session.query(["test.*"], 0, 10)
            retStr += str(dataset.to_df()) + "\n"

            return retStr

    def deleteRow(self):
        retStr = ""
        try:
            # 这里因为key=1的这一行只有test.b.b有值，所以删除test.b.b这一列后这一整行数据就被删除了
            paths = ["test.a.a", "test.a.b", "test.b.b", "test.c.c"]
            timestamps = [5, 6, 7]
            values_list = [
                [None, None, 'a', 'b'],
                ['b', None, None, None],
                ['R', 'E', 'W', 'Q']
            ]
            data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
            self.session.insert_row_records(paths, timestamps, values_list, data_type_list)
            self.session.delete_data("test.b.b", 1, 10)
        except Exception as e:
            if str(e) == (
            "Error occurs: Unable to delete data from read-only nodes. The data of the writable nodes has "
            "been cleared."):
                exit(0)
            print(e)
            exit(1)
        finally:
            # 查询删除后剩余的数据
            dataset = self.session.query(["test.*"], 0, 10)
            retStr += str(dataset.to_df()) + "\n"
            try:
                # 删除部分数据（设置为null
                self.session.batch_delete_data(["test.a.a", "test.a.b"], 5, 7)
            except Exception as e:
                if str(e) == (
                        "Error occurs: Unable to delete data from read-only nodes. The data of the writable nodes has "
                        "been cleared."):
                    exit(0)
                print(e)
                exit(1)
            finally:
                # 查询删除后剩余的数据
                dataset = self.session.query(["test.*"], 0, 10)
                retStr += str(dataset.to_df()) + "\n"

                return retStr

    def downsampleQuery(self):
        import pandas as pd
        try:
            dataset = self.session.downsample_query(["test.*"], start_time=0, end_time=10, type=AggregateType.COUNT,
                                               precision=3)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            retStr = str(dataset.to_df()) + "\n"
        except Exception as e:
            print(e)
            exit(1)


        return retStr

    def downsampleQueryNoInterval(self):
        import pandas as pd
        try:
            dataset = self.session.downsample_query_no_interval(["test.*"], type=AggregateType.COUNT,
                                                    precision=3)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            retStr = str(dataset.to_df()) + "\n"
        except Exception as e:
            print(e)
            exit(1)
        return retStr

    def exportToFile(self):
        try:
            # 将数据存入csv
            self.session.export_to_file("select * from test into outfile \"../generated/output.csv\" as csv with header;")
            # 将数据存入文件
            self.session.export_to_file("select * from test into outfile \"../generated\" as stream;")
        except Exception as e:
            traceback.print_exc()
            print(e)
            exit(1)

        return ""

    def getDebugInfo(self):
        retStr = ""
        dataset = self.session.get_debug_info("".encode(), DebugInfoType.GET_META)
        retStr += dataset.decode() + "\n"
        return retStr

    def insert(self):
        retStr = ""
        try:
            # 写入数据
            paths = ["test.a.a", "test.a.b", "test.b.b", "test.c.c"]
            timestamps = [5, 6, 7]
            values_list = [
                [None, None, 'a', 'b'],
                ['b', None, None, None],
                ['R', 'E', 'W', 'Q']
            ]
            data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
            self.session.insert_row_records(paths, timestamps, values_list, data_type_list)
            # 查询写入的数据
            dataset = self.session.query(["test.*"], 0, 10)
            retStr += str(dataset.to_df()) + "\n"

            paths = ["test.a.a", "test.a.b", "test.b.b"]
            timestamps = [8, 9]
            values_list = [
                [None, 'a', 'b'],
                ['b', None, None]
            ]
            data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY]
            self.session.insert_non_aligned_row_records(paths, timestamps, values_list, data_type_list)
            # 查询写入的数据
            dataset = self.session.query(["test.*"], 0, 10)
            retStr += str(dataset.to_df()) + "\n"

            # 插入列
            paths = ["test.b.c"]
            timestamps = [6]
            values_list = [[1]]
            data_type_list = [DataType.INTEGER]
            self.session.insert_column_records(paths, timestamps, values_list, data_type_list)
            # 查询写入的数据
            dataset = self.session.query(["test.*"], 0, 10)
            retStr += str(dataset.to_df()) + "\n"

            # 插入列
            paths = ["test.b.c"]
            timestamps = [5]
            values_list = [[1]]
            data_type_list = [DataType.INTEGER]
            self.session.insert_non_aligned_column_records(paths, timestamps, values_list, data_type_list)
            # 查询写入的数据
            dataset = self.session.query(["test.*"], 0, 10)
            retStr += str(dataset.to_df()) + "\n"
            return retStr
        except Exception as e:
            print(e)
            exit(1)

    def insertBaseDataset(self):
        try:
            # 查询写入之前的数据
            dataset = self.session.query(["test.*"], 0, 10)
            print('Before insert: ', dataset)

            # 写入数据
            paths = ["test.a.a", "test.a.b", "test.b.b", "test.c.c"]
            timestamps = [0, 1, 2, 3]
            values_list = [
                ['a', 'b', None, None],
                [None, None, 'b', None],
                [None, None, None, 'c'],
                ['Q', 'W', 'E', 'R']
            ]
            data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
            self.session.insert_row_records(paths, timestamps, values_list, data_type_list)
            # 查询写入的数据
            dataset = self.session.query(["test.*"], 0, 10)
            print(dataset)

        except Exception as e:
            traceback.print_exc()
            if str(e) == 'Error occurs: The query results contain overlapped keys.':
                exit(0)
            print(e)
            exit(1)
        finally:

            return ""

    def insertDF(self):
        try:
            import pandas as pd
            data = {
                'key': list(range(10, 20)),
                'value1': ['A']*10,
                'value2': [1.1]*10
            }

            df = pd.DataFrame(data)
            self.session.insert_df(df, "dftestdata")
            data = {
                'key': list(range(10, 20)),
                'dftestdata.value3': ['B']*10,
                'dftestdata.value4': [2.2]*10
            }

            df = pd.DataFrame(data)
            self.session.insert_df(df)

            dataset = self.session.query(["dftestdata.*"], 0, 1000)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            retStr = dataset.to_df().to_string(index=False) + "\n"
            return retStr
        except Exception as e:
            print(e)
            exit(1)

    def lastQuery(self):
        retStr = ""
        # 获取部分序列的最后一个数据点
        dataset = self.session.last_query(["test.*"], 0)
        retStr = str(dataset.to_df()) + "\n"
        return retStr

    def loadCSV(self):
        retStr = ""
        try:
            import os
            path = f"{os.getcwd()}/src/test/resources/pySessionIT/files/a.csv"
            statement = f"LOAD DATA FROM INFILE '{path}' AS CSV INTO test(key, a.a, a.b, b.b, c.c);"
            resp = self.session.load_csv(statement)
            retStr += str(resp) + "\n"

            # 使用 SQL 语句查询写入的数据
            dataset = self.session.query(["test.*"], 0, 10)
            # 转换为pandas.Dataframe
            df = dataset.to_df()
            retStr += str(df) + '\n'

            return retStr
        except Exception as e:
            print(e)
            exit(1)

    def loadDirectory(self):
        retStr = ""
        try:
            # calculate file path
            import os
            path = f"{os.getcwd()}/src/test/resources/pySessionIT/dir"
            self.session.load_directory(path)

            dataset = self.session.query(["dir.*"], 0, 10)
            # 转换为pandas.Dataframe
            retStr += str(dataset.to_df()) + '\n'

            return retStr
        except Exception as e:
            print(e)
            exit(1)

    def query(self):
        retStr = ""

        # 查询写入的数据，数据由PySessionIT测试写入
        dataset = self.session.query(["test.*"], 0, 10)
        # 转换为pandas.Dataframe
        df = dataset.to_df()
        retStr += str(df) + '\n'
        """
           key a.a.a a.a.b a.b.b a.c.c
        0    1  b'a'  b'b'  None  None
        1    2  None  None  b'b'  None
        2    3  None  None  None  b'c'
        3    4  b'Q'  b'W'  b'E'  b'R'
        """
        # 使用 SQL 语句查询写入的数据
        dataset = self.session.execute_statement("select * from test;", fetch_size=2)

        columns = dataset.columns()
        for column in columns:
            retStr += str(column) + '    '
        retStr += '\n'

        while dataset.has_more():
            row = dataset.next()
            for field in row:
                retStr += str(field) + '        '
            retStr += '\n'
        retStr += '\n'

        dataset.close()

        # 使用 SQL 语句查询副本数量
        replicaNum = self.session.get_replica_num()
        retStr += ('replicaNum: ' + str(replicaNum) + '\n')
        
        return retStr

    def showColumns(self):
        retStr = ""
        try:
            # 使用 SQL 语句查询时间序列
            dataset = self.session.execute_statement("SHOW COLUMNS;", fetch_size=2)

            columns = dataset.columns()
            for column in columns:
                retStr += str(column) + "    "
            retStr += "\n"

            while dataset.has_more():
                row = dataset.next()
                for field in row:
                    retStr += str(field) + "        "
                retStr += "\n"
            retStr += "\n"

            dataset.close()
            # 使用 list_time_series() 接口查询时间序列
            timeSeries = self.session.list_time_series()
            for ts in timeSeries:
                retStr += str(ts) + "\n"
            return retStr

        except Exception as e:
            print(e)
            exit(1)
