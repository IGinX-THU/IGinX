from abc import ABC
from .udf import UDF, get_column_index, get_constants
from ..utils.dataframe import DataFrame, pandasDF_to_list, list_to_PandasDF


class UDTF(UDF, ABC):
    """
    参数列举模式
    """

    def __init__(self):
        super().__init__()
        self._key = None

    @property
    def udf_type(self):
        return "UDTF"

    def build_header(self, paths, types):
        # udtf 返回的数据不应该带key, 默认使用原来的列名加函数名前缀，类型不变
        colNames = []
        for name in paths:
            colNames.append(str(self.udf_name) + "(" + name + ")")
        return colNames, types

    def get_key(self):
        return self._key

    def transform(self, data, pos_args, kwargs):
        colNames, types = self.build_header(data[0][1:], data[1][1:])
        df = DataFrame(colNames, types, has_key=False)
        index_list = get_column_index(data[0][1:], pos_args)
        args = [val for arg_type, val in pos_args]
        # 分解每行参数
        for row in data[2:]:
            self._key = row[0]
            for i in range(1, len(row)):
                args[index_list[i-1]] = row[i]
            value = self.eval(*args, **kwargs)
            if not isinstance(value, list):
                value = [value]
            df.insert(*value)
        return df.to_list()


class UDTFinDF(UDF, ABC):
    """
    使用dataframe模式
    """

    @property
    def udf_type(self):
        return "UDTF"

    def build_header(self, paths, types):
        # 用户直接返回dataframe，这个函数应当不需要
        pass

    def transform(self, data, pos_args, kwargs):
        df = list_to_PandasDF(data)
        # 直接作为完整的dataframe进行处理，返回值为dataframe
        res = self.eval(df, *get_constants(pos_args), **kwargs)
        return pandasDF_to_list(res)
