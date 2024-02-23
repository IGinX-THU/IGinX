from abc import ABC, abstractmethod

COLUMN_ARG_TYPE = 0
POS_ARG_TYPE = 1


def get_column_index(paths, pos_args):
    """
    根据位置参数解析dataframe中每一列在udf参数中应该在的位置
    :param paths: dataframe中的列名（保持顺序）
    :param pos_args: 位置参数（二维列表），[[参数类型, 参数值]]。对于列参数，类型为0，参数值为列名；对于常量参数，类型为1，参数值为参数值
    :return: 一个整数组成的列表，按顺序指示每个列所在的参数位置索引（从0开始）
    """
    index_list = []
    for path in paths:
        for i in range(len(pos_args)):
            if pos_args[i][0] == COLUMN_ARG_TYPE and str(pos_args[i][1]) == path:
                index_list.append(i)
    return index_list


def get_constants(pos_args):
    """
    从位置参数中解析常量参数
    :param pos_args: 位置参数（二维列表），[[参数类型, 参数值]]。对于列参数，类型为0，参数值为列名；对于常量参数，类型为1，参数值为参数值
    :return: 一个列表，包含所有常量参数的值，按原序
    """
    return [obj for arg_type, obj in pos_args if arg_type == POS_ARG_TYPE]


class UDF(ABC):
    def __init__(self):
        self._udf_name = None

    @property
    # @abstractmethod
    def udf_name(self):
        # """
        # 用户需要指定udf在sql中的名字以构建返回dataframe
        # TODO: 是否可以让IGInX传递这个信息
        # """
        # pass
        return self._udf_name

    @udf_name.setter
    def udf_name(self, udf_name):
        self._udf_name = udf_name

    def set_udf_name(self, name):
        assert isinstance(name, str)
        self._udf_name = name

    def get_udf_name(self):
        return self._udf_name

    @property
    @abstractmethod
    def udf_type(self):
        """
        三个子类分别实现，返回udf类型
        """

    def get_type(self):
        return self.udf_type

    @abstractmethod
    def transform(self, data, pos_args, kwargs):
        """
        接收IGinX传入的参数，进行预处理后传给用户UDF（通常是重写的transform函数）进行后续处理
        需要拿取用户UDF返回的数据进行检查后返回给IGinX
        :param data: IGinX传入的类dataframe数据，需要包装成IGinX类型的dataframe类
        :param pos_args: 位置参数，包含列名与位置常量参数，二维列表：[[参数类型, 参数值]]。对于列参数，类型为0，参数值为列名；对于常量参数，
        类型为1，参数值为参数值
        :param kwargs: kv参数，字典
        """

    @abstractmethod
    def build_header(self, paths, types):
        """
        按原本的header构建UDF返回的dataframe，
        paths表示列名，types表示列类型，with_key指明是否需要带key列
        :param paths: 原本的列名列表，字符串列表，不带key
        :param types: 原本的列数据类型列表，字符串列表，不带key
        """

    @abstractmethod
    def eval(self, *args, **kwargs):
        """
        用户UDF需要重写这个方法来进行处理操作，使用动态参数是为了匹配不同参数数量的重写函数
        TODO: 或许这里需要进行参数个数的限制（大概只能通过写若干个定死长度的transform函数实现）
        """


