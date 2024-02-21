from abc import ABC
from .udf import UDF, get_constants
from ..utils.dataframe import list_to_PandasDF, pandasDF_to_list


class UDSF(UDF, ABC):
    """
    使用dataframe模式，set to set应当只允许dataframe模式
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
