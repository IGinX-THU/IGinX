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

from enum import Enum

import pandas as pd
import pyarrow as pa

from .thrift.rpc.ttypes import SqlType, AggregateType, ExecuteSqlResp
from .utils.bitmap import Bitmap
from .utils.byte_utils import get_long_array, get_values_by_data_type, BytesParser, get_batch_from_arrow, \
    get_byte_by_type
from .thrift.rpc.ttypes import DataType

def map_dtype(dtype):
    """
    Convert a Pandas.Dataframe data type to IGinX DataType.
    """
    if pd.api.types.is_bool_dtype(dtype):
        return DataType.BOOLEAN
    elif pd.api.types.is_integer_dtype(dtype):
        if dtype == 'int32':
            return DataType.INTEGER
        else:
            return DataType.LONG
    elif pd.api.types.is_float_dtype(dtype):
        if dtype == 'float32':
            return DataType.FLOAT
        else:
            return DataType.DOUBLE
    elif pd.api.types.is_numeric_dtype(dtype):
        return DataType.DOUBLE
    else:
        return DataType.BINARY  # all other types are treated as BINARY


def map_atype(arrow_type):
    """
    Convert a PyArrow data type to IGinX DataType.
    """
    if pa.types.is_boolean(arrow_type):
        return DataType.BOOLEAN

    elif pa.types.is_integer(arrow_type):
        if arrow_type.bit_width == 32:
            return DataType.INTEGER
        elif arrow_type.bit_width == 64:
            return DataType.LONG
        else:
            raise ValueError(f"Unsupported arrow type: {arrow_type}")

    elif pa.types.is_floating(arrow_type):
        if arrow_type == pa.float32():
            return DataType.FLOAT
        elif arrow_type == pa.float64():
            return DataType.DOUBLE
        else:
            raise ValueError(f"Unsupported arrow type: {arrow_type}")

    elif pa.types.is_binary(arrow_type) or pa.types.is_string(arrow_type):
        return DataType.BINARY

    else:
        raise ValueError(f"Unsupported arrow type: {arrow_type}")


# TODO: process NA values
def column_dataset_from_df(df: pd.DataFrame, prefix: str = ""):
    # if no prefix is provided, the column names must contain at least one '.' except key.
    column_list = df.columns.tolist()
    if prefix == "":
        for col in column_list:
            if '.' not in col and col != 'key':
                raise RuntimeError(f"The paths in data must contain '.' or prefix must be set")
    else:
        prefix = prefix + '.'

    if 'key' in column_list:
        # examine key type
        key_dtype = df['key'].dtype
        valid_key = False
        if key_dtype == 'int64':
            valid_key = True
        elif pd.api.types.is_integer_dtype(key_dtype):
            df['key'] = df['key'].astype('int64')
            valid_key = True

        if not valid_key:
            raise RuntimeError(f"Invalid key type is provided. Required long/int but was %s.", df['key'].dtype)
        key_list = df['key'].tolist()
        df = df.drop(columns=['key'])
    # if there is no key column in data, creat one
    else:
        key_list = list(range(len(df)))

    mapped_types = [map_dtype(dtype) for col, dtype in df.dtypes.items()]
    values_list = [df[col].tolist() for col in df.columns]
    column_list = [prefix + col for col in df.columns.tolist()]
    return ColumnDataSet(column_list, mapped_types, key_list, values_list)


class ColumnDataSet(object):
    def __init__(self, paths, types, keys, values_list):
        self.__paths = paths
        self.__types = types
        self.__keys = keys
        self.__values_list = values_list

    def get_insert_args(self):
        return self.__paths, self.__keys, self.__values_list, self.__types

    def __str__(self):
        column_names = ['key'] + self.__paths
        num_rows = len(self.__keys)
        columns = [self.__keys] + self.__values_list
        output = []

        output.append('\t'.join(column_names))

        for i in range(num_rows):
            row = [str(columns[j][i]) for j in range(len(columns))]
            output.append('\t'.join(row))

        return '\n'.join(output)


class Point(object):

    def __init__(self, path, type, timestamp, value):
        self.__path = path
        self.__type = type
        self.__timestamp = timestamp
        self.__value = value

    def get_path(self):
        return self.__path

    def get_type(self):
        return self.__type

    def get_timestamp(self):
        return self.__timestamp

    def get_value(self):
        return self.__value

    def to_df(self):
        df = pd.DataFrame([BytesParser(self.__timestamp).next_long(), BytesParser(self.__value).next(self.__type)],
                          columns=["key", str(self.__path)])
        return df


class QueryDataSet(object):
    def __init__(self, paths, timestamps, values_list, type_list, table:None):
        self.__paths = paths
        self.__timestamps = timestamps
        self.__values = values_list
        self.__table = table # save arrow table for dataframe convertion
        self.__types = type_list


    @classmethod
    def from_old_data(cls, paths, types, timestamps, values_list, bitmap_list):
        paths = paths

        if timestamps is None:
            timestamps = []
        else:
            timestamps = get_long_array(timestamps)

        values = []
        if values_list is not None:
            for i in range(len(values_list)):
                values = []
                bitmap = Bitmap(len(types), bitmap_list[i])
                value_parser = BytesParser(values_list[i])
                for j in range(len(types)):
                    if bitmap.get(j):
                        values.append(value_parser.next(types[j]))
                    else:
                        values.append(None)
                values.append(values)
        return cls(paths, timestamps, values, types, None)

    @classmethod
    def from_arrow(cls, table: pa.Table):
        paths = [col for col in table.column_names if col != "key"]  # remove key
        timestamps = table["key"].to_pylist() if "key" in table.column_names else []
        types = [map_atype(field.type) for field in list(table.schema) if field.name != "key"]
        column_data = [table[col].to_pylist() for col in paths]
        values = list(zip(*column_data))
        return cls(paths, timestamps, values, types, table)

    def get_paths(self):
        return self.__paths

    def get_timestamps(self):
        return self.__timestamps

    def get_values(self):
        return self.__values

    def get_types(self):
        return self.__types

    def get_row_count(self):
        return len(self.__values)

    def has_key(self):
        return self.__timestamps

    def __str__(self):
        value = "Time\t" if self.has_key() else ""
        for path in self.__paths:
            value += path + "\t"
        value += "\n"

        for i in range(len(self.__values)):
            if self.has_key():
                value += str(self.__timestamps[i]) + "\t"
            for j in range(len(self.__paths)):
                if self.__values[i][j] is None:
                    value += "null\t"
                else:
                    value += str(self.__values[i][j]) + "\t"
            value += "\n"
        return value

    def to_df(self):
        if self.__table is not None:
            return self.__table.to_pandas()
        has_key = self.__timestamps != []
        columns = ["key"] if has_key else []
        for column in self.__paths:
            columns.append(str(column))

        value_matrix = []
        if has_key:
            for i in range(len(self.__timestamps)):
                value = [self.__timestamps[i]]
                value.extend(self.__values[i])
                value_matrix.append(value)
        else:
            for i in range(len(self.__values)):
                value_matrix.append(self.__values[i])

        return pd.DataFrame(value_matrix, columns=columns)


class AggregateQueryDataSet(object):

    def __init__(self, paths, timestamps, values, type, table:None):
        self.__type = type
        self.__paths = paths
        self.__timestamps = timestamps
        self.__values = values
        self.__table = table

    @classmethod
    def from_resp(cls, resp, type):
        type = type
        paths = resp.paths
        timestamps = None
        if resp.keys is not None:
            timestamps = get_long_array(resp.keys)
        values = get_values_by_data_type(resp.valuesList, resp.dataTypeList)
        return cls(paths, timestamps, values, type)

    @classmethod
    def from_arrow(cls, table: pa.Table, type):
        paths = [col for col in table.column_names if col != "key"]  # remove key
        timestamps = table["key"].to_pylist() if "key" in table.column_names else None
        column_data = [table[col].to_pylist() for col in paths]
        values = list(zip(*column_data))
        return cls(paths, timestamps, values, type, table)

    def get_type(self):
        return self.__type

    def get_paths(self):
        return self.__paths

    def get_timestamps(self):
        return self.__timestamps

    def get_values(self):
        return self.__values

    def __str__(self):
        value = ""
        if self.__timestamps:
            for i in range(len(self.__timestamps)):
                value += "Time\t" + AggregateType._VALUES_TO_NAMES[self.__type] + "(" + self.__paths[i] + ")\n"
                value += str(self.__timestamps[i]) + "\t" + str(self.__values[i]) + "\n"
        else:
            for path in self.__paths:
                value += AggregateType._VALUES_TO_NAMES[self.__type] + "(" + path + ")\t"
            value += "\n"
            for v in self.__values:
                value += str(v) + "\t"
            value += "\n"
        return value

    def to_df(self):
        if self.__table is not None:
            return self.__table.to_pandas()
        columns = []
        values = []
        # multiple row with different keys, each path, and it's value will be turned into a dataframe
        if self.__timestamps:
            df_list = []
            for i in range(len(self.__timestamps)):
                columns = ["key", AggregateType._VALUES_TO_NAMES[self.__type] + "(" + self.__paths[i] + ")"]
                values = [self.__timestamps[i], self.__values[i]]
                df_list.append(pd.DataFrame(data=[values], columns=columns))
            return df_list
        # no timestamp specified, only need to match paths and its value
        else:
            for path in self.__paths:
                columns.append(AggregateType._VALUES_TO_NAMES[self.__type] + "(" + path + ")")
            for v in self.__values:
                values.append(v)
            return [pd.DataFrame(data=[values], columns=columns)]


class StatementExecuteDataSet(object):
    class State(Enum):
        HAS_MORE = 1,
        NO_MORE = 2,
        UNKNOWN = 3

    def __init__(self, session, query_id, fetch_size, dataset:QueryDataSet, exportStreamDir=None,
                 exportCSV=None):
        self.__session = session
        self.__query_id = query_id
        self.__fetch_size = fetch_size
        self.__dataset = dataset
        self.__state = StatementExecuteDataSet.State.UNKNOWN
        self.__exportStreamDir = exportStreamDir
        self.__exportCSV = exportCSV
        self.__index = 0

    def fetch(self):
        if self.__dataset and self.__index < self.__dataset.get_row_count():
            return

        self.__dataset = None
        self.__index = 0

        tp = self.__session._fetch(self.__query_id, self.__fetch_size)

        if tp[0]:
            self.__state = StatementExecuteDataSet.State.HAS_MORE
        else:
            self.__state = StatementExecuteDataSet.State.NO_MORE

        if tp[1]:
            self.__dataset = QueryDataSet.from_arrow(get_batch_from_arrow(tp[1]))

    def has_more(self):
        if self.__dataset and self.__index < self.__dataset.get_row_count():
            return True

        self.__dataset = None
        self.__index = 0

        if self.__state == StatementExecuteDataSet.State.HAS_MORE or self.__state == StatementExecuteDataSet.State.UNKNOWN:
            self.fetch()

        return self.__dataset

    def next(self):
        if not self.has_more():
            return None

        values = []
        if self.__dataset.has_key():
            values.append(self.__dataset.get_timestamps()[self.__index])
            values.extend(self.__dataset.get_values()[self.__index])
        else:
            values = self.__dataset.get_values()[self.__index]
        self.__index += 1
        return values

    def next_row_as_bytes(self, remove_key=True):
        if not self.has_more():
            return None

        vlist = self.__dataset.get_values()[self.__index]
        tlist = self.__dataset.get_types()
        bytes_list = [get_byte_by_type(vlist[i], tlist[i]) for i in range(len(tlist))]
        if not remove_key:
            bytes_list.insert(0, get_byte_by_type(self.__dataset.get_timestamps()[self.__index], DataType.LONG))
        self.__index += 1
        return bytes_list

    def has_key(self):
        return self.__dataset.has_key()

    def close(self):
        self.__session._close_statement(query_id=self.__query_id)

    def columns(self):
        return self.__dataset.get_paths()

    def types(self):
        return self.__dataset.get_types()

    def get_export_stream_dir(self):
        return self.__exportStreamDir

    def get_export_csv(self):
        return self.__exportCSV
