from abc import ABC, abstractmethod
from udf import UDF, list_to_df
from ..utils.dataframe import DataFrame


class UDAF(UDF, ABC):
    """
    参数列举模式
    """

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
        return DataFrame(colNames, colTypes.extend(types), has_key=with_key)

    # TODO: 目前这样的写法仍然做不到列参数与常量参数混合
    def udf_process(self, data, args, kwargs):
        df = self.build_df_with_header(data[0][1:], data[1][1:])
        # 分解每行参数
        for row in data[2:]:
            row.extend(args)
            self.transform(self.status, *row, **kwargs)
        return df.insert(*self.status)

    @abstractmethod
    def transform(self, status, *args, **kwargs):
        """
        用户UDAF需要重写这个方法来进行处理操作，使用动态参数是为了匹配不同参数数量的重写函数
        status参数为UDAF特有的累计值参数
        :param status: 父类管理的累计值参数
        :param args: 位置参数
        :param kwargs: kv参数
        """


class UDAFinDF(UDF, ABC):
    """
    使用dataframe模式
    """

    def build_df_with_header(self, paths, types, with_key=False):
        # 用户直接返回dataframe，这个函数应当不需要
        pass

    def udf_process(self, data, args, kwargs):
        df = list_to_df(data)
        # 直接作为完整的dataframe进行处理，返回值为dataframe
        res = self.transform(df, *args, **kwargs)
        return res.to_list()
