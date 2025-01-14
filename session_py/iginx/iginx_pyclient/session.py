#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

import csv
import logging
import os.path
import time
from datetime import datetime

import pandas as pd
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
from pathlib import Path

from .cluster_info import ClusterInfo
from .dataset import column_dataset_from_df, QueryDataSet, AggregateQueryDataSet, StatementExecuteDataSet
from .thrift.rpc.IService import Client
from .thrift.rpc.ttypes import (
    OpenSessionReq,
    CloseSessionReq,
    AddUserReq,
    UpdateUserReq,
    DeleteUserReq,
    GetClusterInfoReq,
    GetReplicaNumReq,
    LastQueryReq,
    ShowColumnsReq,
    AddStorageEnginesReq,
    DeleteColumnsReq,
    QueryDataReq,
    DeleteDataInColumnsReq,
    DownsampleQueryReq,
    AggregateQueryReq,
    InsertRowRecordsReq,
    InsertNonAlignedRowRecordsReq,
    InsertColumnRecordsReq,
    InsertNonAlignedColumnRecordsReq,
    ExecuteSqlReq,
    ExecuteStatementReq,
    FetchResultsReq,
    CloseStatementReq,
    DebugInfoReq,
    LoadCSVReq,

    StorageEngine, DataType, FileChunk, UploadFileReq,
)
from .time_series import TimeSeries
from .utils.bitmap import Bitmap
from .utils.byte_utils import timestamps_to_bytes, row_values_to_bytes, column_values_to_bytes, bitmap_to_bytes

logger = logging.getLogger("IginX")

# key value boundary for IGinX(defined in shared.GlobalConstant)
MAX_KEY = 9223372036854775807
MIN_KEY = -9223372036854775807


def isPyReg(statement:str):
    statement = statement.strip().lower()
    return statement.startswith("create") and ("function" in statement)


def process_py_reg(statement:str):
    assert len(statement.split("\"")) >= 7
    path = statement.split("\"")[-1]
    if os.path.isabs(path):
        return statement
    else:
        abs_path = os.path.abspath(path)
        return statement.replace(path, abs_path)


class Session(object):
    SUCCESS_CODE = 200
    DEFAULT_USER = "root"
    DEFAULT_PASSWORD = "root"

    def __init__(self, host, port, user=DEFAULT_USER, password=DEFAULT_PASSWORD):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password

        self.__is_close = True
        self.__transport = None
        self.__client = None
        self.__session_id = None

    def open(self):
        if not self.__is_close:
            return

        self.__transport = TSocket.TSocket(self.__host, self.__port)

        if not self.__transport.isOpen():
            try:
                self.__transport.open()
            except TTransport.TTransportException as e:
                logger.exception("TTransportException!", exc_info=e)

        self.__client = Client(TBinaryProtocol.TBinaryProtocol(self.__transport))

        req = OpenSessionReq(self.__user, self.__password)

        try:
            resp = self.__client.openSession(req)
            Session.verify_status(resp.status)
            self.__session_id = resp.sessionId
            self.__is_close = False
        except Exception as e:
            self.__transport.close()
            logger.exception("session closed because: ", exc_info=e)

    def close(self):
        if self.__is_close:
            return

        req = CloseSessionReq(self.__session_id)
        try:
            self.__client.closeSession(req)
        except TTransport.TException as e:
            logger.exception(
                "Error occurs when closing session. Error message: ",
                exc_info=e,
            )
        finally:
            self.__is_close = True
            if self.__transport is not None:
                self.__transport.close()

    def list_time_series(self):
        req = ShowColumnsReq(sessionId=self.__session_id)
        resp = self.__client.showColumns(req)
        Session.verify_status(resp.status)

        time_series_list = []
        for i in range(len(resp.paths)):
            time_series_list.append(TimeSeries(resp.paths[i], resp.dataTypeList[i]))

        return time_series_list

    def add_storage_engine(self, ip, port, type, extra_params=None):
        self.batch_add_storage_engine([StorageEngine(ip, port, type, extraParams=extra_params)])

    def batch_add_storage_engine(self, storage_engines):
        req = AddStorageEnginesReq(sessionId=self.__session_id, storageEngines=storage_engines)
        status = self.__client.addStorageEngines(req)
        Session.verify_status(status)

    def delete_time_series(self, path):
        self.batch_delete_time_series([path])

    def batch_delete_time_series(self, paths):
        req = DeleteColumnsReq(sessionId=self.__session_id, paths=paths)
        status = self.__client.deleteColumns(req)
        Session.verify_status(status)

    def get_replica_num(self):
        req = GetReplicaNumReq(sessionId=self.__session_id)
        resp = self.__client.getReplicaNum(req)
        Session.verify_status(resp.status)
        return resp.replicaNum

    def insert_row_records(self, paths, timestamps, values_list, data_type_list, tags_list=None, timePrecision=None):
        if len(paths) == 0 or len(timestamps) == 0 or len(values_list) == 0 or len(data_type_list) == 0:
            raise RuntimeError("invalid insert request")
        if len(paths) != len(data_type_list):
            raise RuntimeError("the sizes of paths and data_type_list should be equal")
        if len(timestamps) != len(values_list):
            raise RuntimeError("the sizes of timestamps and values_list should be equal")
        if tags_list is not None and len(tags_list) != len(paths):
            raise RuntimeError("the sizes of paths, values_list, tags_list and data_type_list should be equal")
        if timePrecision is not None and len(timePrecision) == 0:
            raise RuntimeError("invalid timePrecision")

        # 保证时间戳递增
        index = [x for x in range(len(timestamps))]
        index = sorted(index, key=lambda x: timestamps[x])
        timestamps = sorted(timestamps)
        sorted_values_list = []
        for i in range(len(values_list)):
            sorted_values_list.append(values_list[index[i]])

        # 保证序列递增
        index = [x for x in range(len(paths))]
        index = sorted(index, key=lambda x: paths[x])
        paths = sorted(paths)
        # 重排数据类型
        sorted_data_type_list = []
        sorted_tags_list = []
        for i in index:
            sorted_data_type_list.append(data_type_list[i])

        if tags_list is not None:
            for i in index:
                sorted_tags_list.append(tags_list[i])

        # 重排数据
        for i in range(len(sorted_values_list)):
            values = []
            for j in range(len(paths)):
                values.append(sorted_values_list[i][index[j]])
            sorted_values_list[i] = values

        values_buffer_list = []
        bitmap_buffer_list = []
        for i in range(len(timestamps)):
            values = sorted_values_list[i]
            values_buffer_list.append(row_values_to_bytes(values, sorted_data_type_list))
            bitmap = Bitmap(len(values))
            for j in range(len(values)):
                if values[j] is not None:
                    bitmap.set(j)
            bitmap_buffer_list.append(bitmap_to_bytes(bitmap.get_bytes()))

        req = InsertRowRecordsReq(sessionId=self.__session_id, paths=paths, keys=timestamps_to_bytes(timestamps),
                                  valuesList=values_buffer_list,
                                  bitmapList=bitmap_buffer_list, dataTypeList=sorted_data_type_list,
                                  tagsList=sorted_tags_list, timePrecision=timePrecision)
        status = self.__client.insertRowRecords(req)
        Session.verify_status(status)

    def insert_non_aligned_row_records(self, paths, timestamps, values_list, data_type_list, tags_list=None,
                                       timePrecision=None):
        if len(paths) == 0 or len(timestamps) == 0 or len(values_list) == 0 or len(data_type_list) == 0:
            raise RuntimeError("invalid insert request")
        if len(paths) != len(data_type_list):
            raise RuntimeError("the sizes of paths and data_type_list should be equal")
        if len(timestamps) != len(values_list):
            raise RuntimeError("the sizes of timestamps and values_list should be equal")
        if tags_list is not None and len(tags_list) != len(paths):
            raise RuntimeError("the sizes of paths, values_list, tags_list and data_type_list should be equal")
        if timePrecision is not None and len(timePrecision) == 0:
            raise RuntimeError("invalid timePrecision")

        # 保证时间戳递增
        index = [x for x in range(len(timestamps))]
        index = sorted(index, key=lambda x: timestamps[x])
        timestamps = sorted(timestamps)
        sorted_values_list = []
        for i in range(len(values_list)):
            sorted_values_list.append(values_list[index[i]])

        # 保证序列递增
        index = [x for x in range(len(paths))]
        index = sorted(index, key=lambda x: paths[x])
        paths = sorted(paths)
        # 重排数据类型
        sorted_data_type_list = []
        sorted_tags_list = []
        for i in index:
            sorted_data_type_list.append(data_type_list[i])

        if tags_list is not None:
            for i in index:
                sorted_tags_list.append(tags_list[i])

        # 重排数据
        for i in range(len(sorted_values_list)):
            values = []
            for j in range(len(paths)):
                values.append(sorted_values_list[i][index[j]])
            sorted_values_list[i] = values

        values_buffer_list = []
        bitmap_buffer_list = []
        for i in range(len(timestamps)):
            values = sorted_values_list[i]
            values_buffer_list.append(row_values_to_bytes(values, sorted_data_type_list))
            bitmap = Bitmap(len(values))
            for j in range(len(values)):
                if values[j] is not None:
                    bitmap.set(j)
            bitmap_buffer_list.append(bitmap_to_bytes(bitmap.get_bytes()))

        req = InsertNonAlignedRowRecordsReq(sessionId=self.__session_id, paths=paths,
                                            keys=timestamps_to_bytes(timestamps), valuesList=values_buffer_list,
                                            bitmapList=bitmap_buffer_list, dataTypeList=sorted_data_type_list,
                                            tagsList=sorted_tags_list, timePrecision=timePrecision)
        status = self.__client.insertNonAlignedRowRecords(req)
        Session.verify_status(status)

    def insert_column_records(self, paths, timestamps, values_list, data_type_list, tags_list=None, timePrecision=None):
        if len(paths) == 0 or len(timestamps) == 0 or len(values_list) == 0 or len(data_type_list) == 0:
            raise RuntimeError("invalid insert request")
        if len(paths) != len(data_type_list):
            raise RuntimeError("the sizes of paths and data_type_list should be equal")
        if len(paths) != len(values_list):
            raise RuntimeError("the sizes of paths and values_list should be equal")
        if tags_list is not None and len(paths) != len(tags_list):
            raise RuntimeError("the sizes of paths, valuesList, dataTypeList and tagsList should be equal")
        if timePrecision is not None and len(timePrecision) == 0:
            raise RuntimeError("invalid timePrecision")

        # 保证时间戳递增
        index = [x for x in range(len(timestamps))]
        index = sorted(index, key=lambda x: timestamps[x])
        timestamps = sorted(timestamps)
        for i in range(len(values_list)):
            values = []
            for j in range(len(timestamps)):
                values.append(values_list[i][index[j]])
            values_list[i] = values

        # 保证序列递增
        index = [x for x in range(len(paths))]
        index = sorted(index, key=lambda x: paths[x])
        paths = sorted(paths)
        # 重排数据类型
        sorted_values_list = []
        sorted_data_type_list = []
        sorted_tags_list = []
        for i in index:
            sorted_values_list.append(values_list[index[i]])
            sorted_data_type_list.append(data_type_list[index[i]])
        if tags_list is not None:
            for i in index:
                sorted_tags_list.append(tags_list[i])

        values_buffer_list = []
        bitmap_buffer_list = []
        for i in range(len(paths)):
            values = sorted_values_list[i]
            values_buffer_list.append(column_values_to_bytes(values, sorted_data_type_list[i]))
            bitmap = Bitmap(len(timestamps))
            for j in range(len(timestamps)):
                if values[j] is not None:
                    bitmap.set(j)
            bitmap_buffer_list.append(bitmap_to_bytes(bitmap.get_bytes()))

        req = InsertColumnRecordsReq(sessionId=self.__session_id, paths=paths,
                                     keys=timestamps_to_bytes(timestamps),
                                     valuesList=values_buffer_list,
                                     bitmapList=bitmap_buffer_list, dataTypeList=sorted_data_type_list,
                                     tagsList=sorted_tags_list, timePrecision=timePrecision)
        status = self.__client.insertColumnRecords(req)
        Session.verify_status(status)

    def insert_non_aligned_column_records(self, paths, timestamps, values_list, data_type_list, tags_list=None,
                                          timePrecision=None):
        if len(paths) == 0 or len(timestamps) == 0 or len(values_list) == 0 or len(data_type_list) == 0:
            raise RuntimeError("invalid insert request")
        if len(paths) != len(data_type_list):
            raise RuntimeError("the sizes of paths and data_type_list should be equal")
        if len(paths) != len(values_list):
            raise RuntimeError("the sizes of paths and values_list should be equal")
        if tags_list is not None and len(paths) != len(tags_list):
            raise RuntimeError("the sizes of paths, valuesList, dataTypeList and tagsList should be equal")
        if timePrecision is not None and len(timePrecision) == 0:
            raise RuntimeError("invalid timePrecision")

        # 保证时间戳递增
        index = [x for x in range(len(timestamps))]
        index = sorted(index, key=lambda x: timestamps[x])
        timestamps = sorted(timestamps)
        for i in range(len(values_list)):
            values = []
            for j in range(len(timestamps)):
                values.append(values_list[i][index[j]])
            values_list[i] = values

        # 保证序列递增
        index = [x for x in range(len(paths))]
        index = sorted(index, key=lambda x: paths[x])
        paths = sorted(paths)
        # 重排数据类型
        sorted_values_list = []
        sorted_data_type_list = []
        sorted_tags_list = []
        for i in index:
            sorted_values_list.append(values_list[index[i]])
            sorted_data_type_list.append(data_type_list[index[i]])
        if tags_list is not None:
            for i in index:
                sorted_tags_list.append(tags_list[i])

        values_buffer_list = []
        bitmap_buffer_list = []
        for i in range(len(paths)):
            values = sorted_values_list[i]
            values_buffer_list.append(column_values_to_bytes(values, sorted_data_type_list[i]))
            bitmap = Bitmap(len(timestamps))
            for j in range(len(timestamps)):
                if values[j] is not None:
                    bitmap.set(j)
            bitmap_buffer_list.append(bitmap_to_bytes(bitmap.get_bytes()))

        req = InsertNonAlignedColumnRecordsReq(sessionId=self.__session_id, paths=paths,
                                               keys=timestamps_to_bytes(timestamps),
                                               valuesList=values_buffer_list,
                                               bitmapList=bitmap_buffer_list, dataTypeList=sorted_data_type_list,
                                               tagsList=sorted_tags_list, timePrecision=timePrecision)
        status = self.__client.insertNonAlignedColumnRecords(req)
        Session.verify_status(status)

    def insert_df(self, df: pd.DataFrame, prefix: str = ""):
        """
        insert dataframe data into IGinX
        :param df: dataframe that contains data
        :param prefix: (optional) path names in IGinX
               must contain '.'. If columns in dataframe does not meet the requirement, a prefix can be used
        """
        dataset = column_dataset_from_df(df, prefix)
        paths, keys, value_list, type_list = dataset.get_insert_args()
        self.insert_column_records(paths, keys, value_list, type_list)

    def query(self, paths, start_time, end_time, timePrecision=None):
        req = QueryDataReq(sessionId=self.__session_id, paths=Session.merge_and_sort_paths(paths),
                           startKey=start_time, endKey=end_time, timePrecision=timePrecision)
        resp = self.__client.queryData(req)
        Session.verify_status(resp.status)
        paths = resp.paths
        data_types = resp.dataTypeList
        raw_data_set = resp.queryDataSet
        return QueryDataSet(paths, data_types, raw_data_set.keys, raw_data_set.valuesList, raw_data_set.bitmapList)

    def last_query(self, paths, start_time=0, timePrecision=None):
        if len(paths) == 0:
            logger.warning("paths shouldn't be empty")
            return None
        req = LastQueryReq(sessionId=self.__session_id, paths=Session.merge_and_sort_paths(paths), startKey=start_time,
                           timePrecision=timePrecision)
        resp = self.__client.lastQuery(req)
        Session.verify_status(resp.status)
        paths = resp.paths
        data_types = resp.dataTypeList
        raw_data_set = resp.queryDataSet
        return QueryDataSet(paths, data_types, raw_data_set.keys, raw_data_set.valuesList,
                            raw_data_set.bitmapList)

    def downsample_query_no_interval(self, paths, type, precision, timePrecision=None):
        return self.downsample_query(paths, MIN_KEY, MAX_KEY, type, precision, timePrecision)


    def downsample_query(self, paths, start_time, end_time, type, precision, timePrecision=None):
        req = DownsampleQueryReq(sessionId=self.__session_id, paths=paths, startKey=start_time, endKey=end_time,
                                 aggregateType=type,
                                 precision=precision, timePrecision=timePrecision)
        resp = self.__client.downsampleQuery(req)
        Session.verify_status(resp.status)
        paths = resp.paths
        data_types = resp.dataTypeList
        raw_data_set = resp.queryDataSet
        return QueryDataSet(paths, data_types, raw_data_set.keys, raw_data_set.valuesList,
                            raw_data_set.bitmapList)

    def aggregate_query(self, paths, start_time, end_time, type, timePrecision=None):
        req = AggregateQueryReq(sessionId=self.__session_id, paths=paths, startKey=start_time, endKey=end_time,
                                aggregateType=type, timePrecision=timePrecision)
        resp = self.__client.aggregateQuery(req)
        Session.verify_status(resp.status)
        return AggregateQueryDataSet(resp=resp, type=type)

    def delete_data(self, path, start_time, end_time, timePrecision=None):
        self.batch_delete_data([path], start_time, end_time, timePrecision)

    def batch_delete_data(self, paths, start_time, end_time, timePrecision=None):
        req = DeleteDataInColumnsReq(sessionId=self.__session_id, paths=paths, startKey=start_time, endKey=end_time,
                                     timePrecision=timePrecision)
        status = self.__client.deleteDataInColumns(req)
        Session.verify_status(status)

    def add_user(self, username, password, auths):
        req = AddUserReq(sessionId=self.__session_id, username=username, password=password, auths=auths)
        status = self.__client.addUser(req)
        Session.verify_status(status)

    def delete_user(self, username):
        req = DeleteUserReq(sessionId=self.__session_id, username=username)
        status = self.__client.deleteUser(req)
        Session.verify_status(status)

    def update_user(self, username, password=None, auths=None):
        req = UpdateUserReq(sessionId=self.__session_id, username=username)
        if password:
            req.password = password
        if auths:
            req.auths = auths
        status = self.__client.updateUser(req)
        Session.verify_status(status)

    def get_cluster_info(self):
        req = GetClusterInfoReq(sessionId=self.__session_id)
        resp = self.__client.getClusterInfo(req)
        Session.verify_status(resp.status)
        return ClusterInfo(resp)

    def execute_sql(self, statement):
        if isPyReg(statement):
            statement = process_py_reg(statement)
        req = ExecuteSqlReq(sessionId=self.__session_id, statement=statement)
        resp = self.__client.executeSql(req)
        Session.verify_status(resp.status)
        return resp

    def execute_statement(self, statement, fetch_size=2147483647):
        req = ExecuteStatementReq(sessionId=self.__session_id, statement=statement, fetchSize=fetch_size)
        resp = self.__client.executeStatement(req)
        Session.verify_status(resp.status)
        return StatementExecuteDataSet(self, resp.queryId, resp.columns, resp.dataTypeList, fetch_size,
                                       resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList, resp.exportStreamDir,
                                       resp.exportCSV)

    def load_csv(self, statement):
        req = ExecuteSqlReq(sessionId=self.__session_id, statement=statement)
        resp = self.__client.executeSql(req)
        Session.verify_status(resp.status)

        path = resp.loadCsvPath
        file = Path(path)
        if not file.is_file():
            raise ValueError(f"{path} is not a file!")
        if not path.endswith(".csv"):
            raise ValueError(f"The file name must end with [.csv], {path} doesn't satisfy the requirement!")

        chunk_size = 1024 * 1024
        filename = str(time.time() * 1000) + ".csv"
        with open(file, 'rb') as f:
            offset = 0
            while chunk := f.read(chunk_size):
                chunk_data = FileChunk(
                    fileName=filename,
                    offset=offset,
                    data=chunk,
                    chunkSize=len(chunk)
                )
                req = UploadFileReq(sessionId=self.__session_id, fileChunk=chunk_data)
                resp = self.__client.uploadFileChunk(req)
                Session.verify_status(resp.status)
                offset += len(chunk)

        req = LoadCSVReq(sessionId=self.__session_id, statement=statement, csvFileName=filename)
        resp = self.__client.loadCSV(req)
        Session.verify_status(resp.status)
        return resp

    # load all files in a directory into IGinX database
    def load_directory(self, dir_path, chunk_size=1024 * 10):
        all_files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if
                     os.path.isfile(os.path.join(dir_path, f))]
        for file_path in all_files:
            print(f"Reading file: {file_path}")
            self.load_file_by_chunks(file_path, chunk_size)
            print(f"Load file: {file_path} into IGinX succeeded.")

    # divide file into chunks(default:10KB)
    def load_file_by_chunks(self, file_path, chunk_size):
        step = self.decide_log_step(file_path)
        with open(file_path, 'rb') as file:
            index = 0
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                self.process_file_chunk(chunk, file_path, index, index % step == 0)
                index += 1

    def decide_log_step(self, file_path):
        file_size = Path(file_path).stat().st_size
        # 100MB以内
        if file_size < 100 * 1024 * 1024:
            return 10  # 每10块记录一次
        # 100MB到1GB
        elif file_size < 1 * 1024 * 1024 * 1024:
            return 100  # 每100块记录一次
        # 大于1GB
        else:
            return 1000  # 每1000块记录一次


    # insert file chunk data into IGinX
    # index will be key, chunk will be value
    def process_file_chunk(self, chunk, file_path, index, is_report):
        file_name = os.path.basename(file_path).replace("-", "_")
        file_dir = os.path.basename(os.path.dirname(file_path)).replace("-", "_")
        if file_name.__contains__("-") or file_dir.__contains__("-"):
            print(f"Warning occurred processing file {file_path}: File name: {file_name} and last dir: {file_dir}/ can't contain \"-\"")
            print(f"\t\t \"-\" will be replaced with \"_\"")
            file_name = file_name.replace("-", "_")
            file_dir = file_dir.replace("-", "_")
        data_path = file_dir + "." + str(file_name).replace(".", "_")
        # 若干个块一报，避免大文件报太多
        if is_report:
            print(f"Processing #{index} file chunk and more chunks...")
        self.insert_row_records(paths=[data_path], timestamps=[index], values_list=[[chunk]],
                                data_type_list=[DataType.BINARY])

    def export_to_file(self, statement):
        resp = self.execute_statement(statement)
        if resp.get_export_stream_dir():
            self.export_stream(resp)
        if resp.get_export_csv():
            self.export_csv(resp)

    def export_stream(self, resp):
        dir_path = resp.get_export_stream_dir()

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        if not os.path.isdir(dir_path):
            raise ValueError(f"{dir_path} is not a directory!")

        final_cnt = len(resp.columns())
        columns = list(resp.columns())
        count_map = {}

        for i in range(len(columns)):
            origin_column = columns[i]
            if origin_column == "key":
                columns[i] = ""
                final_cnt -= 1
                continue

            count = count_map.get(origin_column, 0)
            count += 1
            count_map[origin_column] = count

            # 重复的列名在列名后面加上(1),(2)...
            if count >= 2:
                columns[i] = os.path.join(dir_path, f"{origin_column}({count - 1})")
            else:
                columns[i] = os.path.join(dir_path, origin_column)

            # 若将要写入的文件存在，删除之
            if os.path.exists(columns[i]):
                os.remove(columns[i])

        while resp.has_more():
            cache = self.cache_result_byte_array(resp, True)
            self.export_byte_stream(cache, columns)

        resp.close()

        print(f"Successfully wrote {final_cnt} file(s) to directory: \"{os.path.abspath(dir_path)}\".")

    def export_byte_stream(self, binary_data_list, columns):
        for i, column in enumerate(columns):
            if not column:
                continue

            try:
                # 如果文件不存在，则创建并打开文件；否则，以追加模式打开文件
                mode = 'ab' if os.path.exists(column) else 'wb'
                with open(column, mode) as fos:
                    for value in binary_data_list:
                        # 写入对应列的数据
                        fos.write(value[i-1])
            except IOError as e:
                raise RuntimeError(f"Encounter an error when writing file {column}, because {e}")

    def cache_result_byte_array(self, dataset, remove_key):
        cache = []
        row_index = 0
        fetch_size = 1000
        types = list(dataset.types())
        if remove_key:
            types.pop(0)

        while dataset.has_more() and row_index < fetch_size:
            byte_value = dataset.next_row_as_bytes(True)
            if byte_value:
                cache.append(byte_value)
            row_index += 1
        return cache

    # 仿照java代码编写，未经测试，需要注意
    def export_csv(self, resp):
        export_csv = resp.get_export_csv()

        path = export_csv.exportCsvPath
        if not path.endswith(".csv"):
            raise ValueError(f"The file name must end with [.csv], {path} doesn't satisfy the requirement!")

        # 删除原来的csv文件，新建一个新的csv文件
        if os.path.exists(path):
            os.remove(path)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(path, 'w', newline='') as file:  # newline='' 是为了防止在Windows上写入额外的空行
            writer = csv.writer(file)
            has_key = resp.columns()[0] == "key"

            if export_csv.isExportHeader:
                header_names = ["key"] if not has_key else []
                header_names.extend(resp.columns())
                writer.writerow(header_names)

            index = 0
            while resp.has_more():
                index += 1
                print(f"Writing #{index} 1000 records into file: {os.path.abspath(path)}...")
                cache = self.cache_result(resp, True)
                writer.writerows(cache)

        print(f"Successfully wrote csv file: \"{os.path.abspath(path)}\".")

    def cache_result(self, dataset, skip_header):
        has_key = dataset.columns()[0] == "key"
        cache = []
        if not skip_header:
            cache.append(dataset.columns())

        row_index = 0
        fetch_size = 1000

        while dataset.has_more() and row_index < fetch_size:
            row = dataset.next()
            if row:
                str_row = []
                if has_key:
                    key = datetime.fromtimestamp(row[0]).strftime("%Y-%m-%d %H:%M:%S")
                    str_row.append(key)
                    for i in range(1, len(row)):
                        if isinstance(row[i], bytes):
                            str_row.append(row[i].decode("utf-8"))
                        else:
                            str_row.append(str(row[i]))
                else:
                    str_row = [value.decode("utf-8") if isinstance(value, bytes) else str(value) for value in row]
                cache.append(str_row)
                row_index += 1

        return cache

    def get_debug_info(self, payload, typ):
        req = DebugInfoReq(payload=payload, payloadType=typ)
        resp = self.__client.debugInfo(req)
        Session.verify_status(resp.status)
        return resp.payload

    def _fetch(self, query_id, fetch_size):
        req = FetchResultsReq(sessionId=self.__session_id, queryId=query_id, fetchSize=fetch_size)
        resp = self.__client.fetchResults(req)
        Session.verify_status(resp.status)
        return (resp.hasMoreResults, resp.queryDataSet)

    def _close_statement(self, query_id):
        req = CloseStatementReq(sessionId=self.__session_id, queryId=query_id)
        status = self.__client.closeStatement(req)
        Session.verify_status(status)

    @staticmethod
    def verify_status(status):
        if status.code != Session.SUCCESS_CODE:
            raise RuntimeError("Error occurs: " + status.message)

    @staticmethod
    def merge_and_sort_paths(paths):
        for path in paths:
            if path == '*':
                return ['*']

        prefixes = []
        for path in paths:
            index = path.find('*')
            if index != -1:
                prefixes.append(path[:index])

        if len(prefixes) == 0:
            return sorted(paths)

        merged_paths = []
        for path in paths:
            if '*' not in path:
                skip = False
                for prefix in prefixes:
                    if path.startswith(prefix):
                        skip = True
                        break
                if skip:
                    continue
            merged_paths.append(path)

        return sorted(merged_paths)
