from abc import ABC, abstractmethod
from udf_funcs.python_scripts.iginx_udf.utils.dataframe import DataFrame


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
    return DataFrame(paths, types, key_list, value_list, True)


class UDF(ABC):
    def __init__(self):
        self._udf_type = None

    @property
    @abstractmethod
    def udf_name(self):
        """
        用户需要指定udf在sql中的名字以构建返回dataframe
        TODO: 是否可以让IGInX传递这个信息
        """
        pass

    @property
    def udf_type(self):
        return self._udf_type

    @udf_type.setter
    def udf_type(self, value):
        self._udf_type = value

    @abstractmethod
    def udf_process(self, data, args, kwargs):
        """
        接收IGinX传入的参数，进行预处理后传给用户UDF（通常是重写的transform函数）进行后续处理
        需要拿取用户UDF返回的数据进行检查后返回给IGinX
        :param data: IGinX传入的类dataframe数据，需要包装成IGinX类型的dataframe类
        :param args: 位置参数，列表
        :param kwargs: kv参数，字典
        """

    @abstractmethod
    def build_df_with_header(self, paths, types, with_key):
        """
        按原本的header构建UDF返回的dataframe，
        paths表示列名，types表示列类型，with_key指明是否需要带key列
        :param paths: 原本的列名列表，字符串列表，不带key
        :param types: 原本的列数据类型列表，字符串列表，不带key
        :param with_key: 是否要带列名，boolean
        """

    @abstractmethod
    def transform(self, *args, **kwargs):
        """
        用户UDF需要重写这个方法来进行处理操作，使用动态参数是为了匹配不同参数数量的重写函数
        TODO: 或许这里需要进行参数个数的限制（大概只能通过写若干个定死长度的transform函数实现）
        :param args: 位置参数
        :param kwargs: kv参数
        """
