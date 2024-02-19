from abc import ABC, abstractmethod
from .udf import UDF, get_column_index, get_constants
from ..utils.dataframe import DataFrame, list_to_df, pandasDF_to_list


class UDAF(UDF, ABC):
    """
    参数列举模式
    """

    @property
    def udf_type(self):
        return "UDAF"

    def get_key(self):
        return self._key

    @property
    @abstractmethod
    def status(self):
        """
        聚合操作时的累计值，用户必须指定，因为用户UDF只用于一行，累计值必须父类管理
        status需要为元组或列表，并其值的顺序与build header函数中的列名一一对应
        """
        pass

    def build_df_with_header(self, paths, types, with_key=False):
        # udaf 返回的数据可以带key也可以不带，默认不带
        colNames = ["key"] if with_key else []
        colTypes = ["LONG"] if with_key else []
        for name in paths:
            colNames.append(self.udf_name + "(" + name + ")")
        return colNames, colTypes.extend(types), with_key

    def transform(self, data, pos_args, kwargs):
        colNames, types, with_key = self.build_df_with_header(data[0][1:], data[1][1:])
        df = DataFrame(colNames, types, has_key=with_key)
        index_list = get_column_index(data[0][1:], pos_args)
        args = [self.status]
        args.extend([val for arg_type, val in pos_args])
        # 分解每行参数
        for row in data[2:]:
            self._key = row[0]
            for i in range(1, len(row)):
                args[index_list[i]] = row[i]
            args.insert(0, self.status)
            self.eval(*args, **kwargs)
        df.insert(*self.status)
        return df.to_list()

    # @abstractmethod
    # def eval(self, status, *args, **kwargs):
    #     """
    #     用户UDAF需要重写这个方法来进行处理操作，使用动态参数是为了匹配不同参数数量的重写函数
    #     status参数为UDAF特有的累计值参数
    #     :param status: 父类管理的累计值参数
    #     :param args: 位置参数
    #     :param kwargs: kv参数
    #     """


class UDAFinDF(UDF, ABC):
    """
    使用dataframe模式
    """

    @property
    def udf_type(self):
        return "UDAF"

    def build_df_with_header(self, paths, types, with_key=False):
        # 用户直接返回dataframe，这个函数应当不需要
        pass

    def transform(self, data, pos_args, kwargs):
        df = list_to_df(data)
        # 直接作为完整的dataframe进行处理，返回值为dataframe
        res = self.eval(df.to_pandas_df(), *get_constants(pos_args), **kwargs)
        return pandasDF_to_list(res)
