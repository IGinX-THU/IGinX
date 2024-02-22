from abc import ABC, abstractmethod
from .udf import UDF, get_column_index, get_constants
from ..utils.dataframe import DataFrame, pandasDF_to_list, list_to_PandasDF


class UDAF(UDF, ABC):
    """
    参数列举模式
    """
    def __init__(self):
        super().__init__()
        self._key = None
        self.status = self.init_status()

    @property
    def udf_type(self):
        return "UDAF"

    def get_key(self):
        return self._key

    @abstractmethod
    def init_status(self):
        """
        初始化累计值status，需要返回status的初始值
        :return 累计值status的初始值
        """

    def build_header(self, paths, types):
        # udaf 返回的数据可以带key也可以不带，默认不带
        colNames = []
        colTypes = []
        for name in paths:
            colNames.append(self.udf_name + "(" + name + ")")
        return colNames, colTypes.extend(types)

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
            self.status = self.eval(self.status, *args, **kwargs)
        if isinstance(self.status, list):
            df.insert(*self.status)
        else:
            df.insert(self.status)
        return df.to_list()


class UDAFinDF(UDF, ABC):
    """
    使用dataframe模式
    """

    @property
    def udf_type(self):
        return "UDAF"

    def build_header(self, paths, types):
        # 用户直接返回dataframe，这个函数应当不需要
        pass

    def transform(self, data, pos_args, kwargs):
        df = list_to_PandasDF(data)
        # 直接作为完整的dataframe进行处理，返回值为dataframe
        res = self.eval(df, *get_constants(pos_args), **kwargs)
        return pandasDF_to_list(res)
