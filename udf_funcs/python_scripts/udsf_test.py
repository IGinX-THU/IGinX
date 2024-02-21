import pandas as pd

from iginx_udf import UDSF


def to_str(obj):
    if isinstance(obj, bytes):
        return str(obj, encoding='utf-8')
    else:
        return str(obj)


class UDSFTest(UDSF):
    # @property
    # def udf_name(self):
    #     return "udsf_test"

    def eval(self, data, val1='val1', val2='val2'):
        data = data.drop(columns=['key'])
        res_list = []
        print(self.udf_name)
        for row in data.itertuples(index=False):
            s = ''
            for value in row:
                s += to_str(value)
            s += to_str(val1) + to_str(val2)
            print(s)
            res_list.append(s)
        cols = ', '.join(data.columns.tolist())
        col = f"{self.udf_name}({cols})"
        return pd.DataFrame(data={
            col: res_list
        })


"""
from udsf_test import UDSFTest
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"],[1,b'test1'],[1,3]]
# pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDSFTest()
test.transform(data,pos_args,kwargs)

insert into test(key,a) values(0,1),(1,2);
insert into test(key,a,b) values(0,1,2),(1,2,3),(2,3,4);
REGISTER UDSF PYTHON TASK "UDSFTest" IN "udf_funcs\\python_scripts\\udsf_test.py" AS "udsf_test";
select udsf_test(a,b) from test;
select udsf_test(b,a) from test;
select udsf_test(a,b,"test1") from test;
select udsf_test(a,b,"test1","test2") from test;
select udsf_test(a,b,val1="test1") from test;
select udsf_test(a,b,val2="test2") from test;
select udsf_test(a,b,val1="test1",val2="test2") from test;
"""
