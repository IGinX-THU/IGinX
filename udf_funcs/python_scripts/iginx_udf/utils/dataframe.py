import pandas as pd


def list_to_df(data):
    """
    将IGinX传来的二维列表转换为dataframe类数据
    :param data: 二维列表，第一行列名，第二行类型，第三行开始为数据
    :return: 构建的dataframe类数据
    """
    key_list = []
    value_list = []
    paths = data[0][1:]
    types = data[1][1:]
    for row in data[2:]:
        key_list.append(row[0])
        value_list.append(row[1:])
    return DataFrame(paths, types, key_list, value_list, True).to_pandas_df()


def list_to_PandasDF(data):
    """
    将IGinX传来的二维列表转换为Pandas.Dataframe类数据
    :param data: 二维列表，第一行列名，第二行类型，第三行开始为数据
    :return: 构建的dataframe
    """
    return pd.DataFrame(data[2:], columns=data[0])


def pandasDF_to_list(df):
    """
    将一个pandas.dataframe转为二维列表传回给IGinX
    TODO：在大数据量情况下性能表现可能不佳
    :param df: 待转换的dataframe数据
    :return 二维列表，第一行列名，第二行类型，第三行开始为数据
    """
    column_types = df.dtypes
    col_name = [column_name for column_name, data_type in column_types.items()]
    col_type = [map_to_strings(data_type) for column_name, data_type in column_types.items()]
    resList = [col_name, col_type]
    for index, row in df.iterrows():
        resList.append([value for column_name, value in row.items()])
    return resList


def map_to_strings(data_type):
    type_mapping = {
        'int32': 'INTEGER',
        'int64': 'LONG',
        'float32': 'FLOAT',
        'float64': 'DOUBLE',
        'object': 'BINARY',
        'bool': 'Boolean',
    }
    mapped_type = type_mapping.get(str(data_type), 'Unknown')
    return mapped_type


class DataFrame:
    def __init__(self, paths, types, keys=None, values_list=None, has_key=False):
        if keys is None:
            keys = []
        if values_list is None:
            values_list = []
        self._has_key = has_key
        self._paths = paths
        self._types = types
        self._keys = keys
        self._values = values_list

    def get_paths(self):
        return self._paths

    def get_keys(self):
        return self._keys

    def get_values(self):
        return self._values

    def get_values_by_key(self, key):
        if key not in self._keys:
            raise ValueError(f"Error: Cannot find key {key} in data.")
        else:
            return self._values[self._keys.index(key)]

    def insert(self, *args, **kwargs):
        # TODO:类型检查
        if len(args) == 0:
            # 通过指定列名=值来插入数据
            if "key" in kwargs and not self._has_key:
                # 如果指定key值，只有在has_key为True或者首次插入数据时才允许，这是为了兼容UDF返回的无key的dataframe
                if len(self._values) == 0:
                    self._has_key = True
                else:
                    raise ValueError(f"Error: Can't add key into dataframe because data without key exists.")
            elif "key" not in kwargs and self._has_key:
                # 设置了has_key为True但没有给key值
                raise ValueError(f"Error: Key required when inserting.")
            offset = 1 if self._has_key else 0
            row_index = -1
            value_list = [None] * len(kwargs)
            # 根据列名排列值
            for path in kwargs:
                if path == "key":
                    value_list[0] = kwargs[path]
                    if kwargs[path] in self._keys:
                        row_index = self._keys.index(kwargs[path])
                    continue
                if path in self._paths:
                    value_list[self._paths.index(path) + offset] = kwargs[path]
                else:
                    raise ValueError(f"Error: dataframe does not have column named {path}")
            # 如果key已存在，更新指定值
            if row_index == -1:
                self._values.append(value_list)
            else:
                for i in range(len(value_list)):
                    if value_list[i] is not None:
                        self._values[row_index][i] = value_list[i]
        elif self._has_key and len(args) != len(self._paths) + 1:
            raise ValueError(f"Error: Expecting {len(self._paths) + 1} values to insert")
        elif (not self._has_key) and len(args) != len(self._paths):
            raise ValueError(f"Error: Expecting {len(self._paths)} values to insert")
        else:
            # 直接列举每列的值，按位置进行匹配，同样分有key和无key两种
            if self._has_key:
                if args[0] in self._keys:
                    self._values[self._keys.index(args[0])] = args[1:]
                else:
                    self._keys.append(args[0])
                    self._values.append(args[1:])
            else:
                self._values.append(args)

    def to_pandas_df(self):
        columns = ["key"] if self._has_key else []
        for column in self._paths:
            columns.append(str(column))

        value_matrix = []
        for i in range(len(self._values)):
            value = [self._keys[i]] if self._has_key else []
            value.extend(self._values[i])
            value_matrix.append(value)

        return pd.DataFrame(value_matrix, columns=columns)

    def to_list(self):
        names = ["key"] if self._has_key else []
        names.extend(self._paths)
        types = ["LONG"] if self._has_key else []
        types.extend(self._types)
        res = [names, types]
        for i in range(len(self._values)):
            value = [self._keys[i]] if self._has_key else []
            value.extend(self._values[i])
            res.append(value)
        return res

    def __str__(self):
        value = "Key\t" if self._has_key else ""
        for path in self._paths:
            value += path + "\t"
        value += "\n"

        for i in range(len(self._values)):
            value += str(self._keys[i]) + "\t" if self._has_key else ""
            for j in range(len(self._paths)):
                if self._values[i][j] is None:
                    value += "null\t"
                else:
                    value += str(self._values[i][j]) + "\t"
            value += "\n"
        return value
