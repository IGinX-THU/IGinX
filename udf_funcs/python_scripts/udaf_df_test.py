import pandas as pd
from iginx_udf import UDAFinDF


class UDAFinDFTest(UDAFinDF):
    @property
    def udf_name(self):
        return "udaf_df_test"

    def eval(self, data, weight_a=1, weight_b=1):
        # 使用itertuples遍历DataFrame的每一行
        data = data.drop(columns=['key'])
        sum = 0.0
        for row in data.itertuples(index=False):
            # 获取每一行的第一和第二个元素
            a = row[0]
            b = row[1]
            sum += float(a * weight_a + b * weight_b)
        s = ', '.join(data.columns.tolist())
        col = f"udaf_df_test({s})"
        return pd.DataFrame(data={
            col: [sum]
        })



"""
df2 = pd.DataFrame(columns=["udaf_test(test.a, test.b)"])

from udaf_df_test import UDAFinDFTest
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"],[1,2],[1,3]]
kwargs = {}
test = UDAFinDFTest()
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
REGISTER UDAF PYTHON TASK "UDAFinDFTest" IN "udf_funcs\\python_scripts\\udaf_df_test.py" AS "udaf_df_test";

select udaf_df_test(a,b) from test;
select udaf_df_test(a,b,2) from test;
select udaf_df_test(a,b,2,3) from test;
select udaf_df_test(a,b,weight_a=2) from test;
select udaf_df_test(a,b,weight_b=3) from test;
select udaf_df_test(a,b,weight_a=2, weight_b=3) from test;
"""
