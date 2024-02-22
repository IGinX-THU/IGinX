import pandas as pd

from iginx_udf import UDTFinDF


def to_str(obj):
    if isinstance(obj, bytes):
        return str(obj, encoding='utf-8')
    else:
        return str(obj)


class UDTFinDFTest(UDTFinDF):
    @property
    def udf_name(self):
        return "udtf_df_test"

    def eval(self, data, val1='val1', val2='val2'):
        data = data.drop(columns=['key'])
        res_list = []
        for row in data.itertuples(index=False):
            s = ''
            for value in row:
                s += to_str(value)
            s += to_str(val1) + to_str(val2)
            res_list.append(s)
        cols = ', '.join(data.columns.tolist())
        col = f"udtf_df_test({cols})"
        return pd.DataFrame(data={
            col: res_list
        })


"""
from udtf_df_test import UDTFinDFTest
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"],[1,b'test1'],[1,3]]
# pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDTFinDFTest()
test.transform(data,pos_args,kwargs)

insert into test(key,a) values(0,1),(1,2);
insert into test(key,a,b) values(0,1,2),(1,2,3),(2,3,4);
REGISTER UDF PYTHON TASK "UDTFinDFTest" IN "udf_funcs\\python_scripts\\udtf_df_test.py" AS "udtf_df_test";

select udtf_df_test(a,b) from test;
select udtf_df_test(b,a) from test;
select udtf_df_test(a,b,"test1") from test;
select udtf_df_test(a,b,"test1","test2") from test;
select udtf_df_test(a,b,val1="test1") from test;
select udtf_df_test(a,b,val2="test2") from test;
select udtf_df_test(a,b,val1="test1",val2="test2") from test;
"""

