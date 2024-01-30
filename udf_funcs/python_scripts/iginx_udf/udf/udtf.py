from abc import ABC
from udf import UDF, list_to_df
from ..utils.dataframe import DataFrame


class UDTF(UDF, ABC):
    """
    参数列举模式
    """
    def build_df_with_header(self, paths, types, with_key=False):
        # udtf 返回的数据不应该带key, 默认使用原来的列名加函数名前缀，类型不变
        colNames = []
        for name in paths:
            colNames.append(self.udf_name + "(" + name + ")")
        return DataFrame(colNames, types, has_key=False)

    # TODO: 目前这样的写法仍然做不到列参数与常量参数混合
    def udf_process(self, data, args, kwargs):
        df = self.build_df_with_header(data[0][1:], data[1][1:])
        # 分解每行参数
        for row in data[2:]:
            row.extend(args)
            value = list(self.transform(*row, **kwargs))
            df.insert(*value)
        return df.to_list()


class UDTFinDF(UDF, ABC):
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
