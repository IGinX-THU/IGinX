from abc import ABC
from udf import UDF, list_to_df
from ..utils.dataframe import DataFrame


class UDSF(UDF, ABC):
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
