import sys

sys.path.append('../session_py/')  # 将上一级目录添加到Python模块搜索路径中

from iginx.iginx_pyclient.session import Session


class Query:
    def __init__(self):
        pass

    def test(self):
        retStr = ""
        session = Session('127.0.0.1', 6888, "root", "root")
        session.open()

        # 查询写入的数据，数据由PySessionIT测试写入
        dataset = session.query(["test.*"], 0, 10)
        retStr += str(dataset)
        # 转换为pandas.Dataframe
        df = dataset.to_df()
        retStr += str(df) + '\n'
        """
           key a.a.a a.a.b a.b.b a.c.c
        0    1  b'a'  b'b'  None  None
        1    2  None  None  b'b'  None
        2    3  None  None  None  b'c'
        3    4  b'Q'  b'W'  b'E'  b'R'
        """
        # 使用 SQL 语句查询写入的数据
        dataset = session.execute_statement("select * from test;", fetch_size=2)

        columns = dataset.columns()
        for column in columns:
            retStr += str(column) + '\t'
        retStr += '\n'

        while dataset.has_more():
            row = dataset.next()
            for field in row:
                retStr += str(field) + '\t\t'
            retStr += '\n'
        retStr += '\n'

        dataset.close()

        # 使用 SQL 语句查询副本数量
        replicaNum = session.get_replica_num()

        retStr += ('replicaNum: ' + str(replicaNum) + '\n')

        dataset.close()

        session.close()
        return retStr
