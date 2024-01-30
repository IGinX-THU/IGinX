import pandas as pd


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


class Column:
    def __init__(self, path, col_type, value_list=None):
        if isinstance(path, str):
            self._path = path
        else:
            raise TypeError("Error: Column path should be string.")

        if isinstance(col_type, str):
            self._type = col_type
        else:
            raise TypeError("Error: Column type should be string.")

        if value_list is None:
            self._value = []
        elif isinstance(value_list, list):
            self._value = value_list
        else:
            raise TypeError("Error: Column value should be null or list.")

    def set_value(self, value_list):
        if isinstance(value_list, list):
            self._value = value_list
        else:
            raise TypeError("Error: Column value should be list to set.")

    def get_value(self):
        return self._value

    def set_path(self, path):
        if isinstance(path, list):
            self._path = path
        else:
            raise TypeError("Column path should be string.")

    def get_path(self):
        return self._path

    def set_type(self, col_type):
        if isinstance(col_type, list):
            self._type = col_type
        else:
            raise TypeError("Column type should be string.")

    def get_type(self):
        return self._type
