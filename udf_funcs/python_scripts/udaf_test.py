from iginx_udf import UDAF


class UDAFTest(UDAF):
    @property
    def udf_name(self):
        return "udaf_test"

    def init_status(self):
        return 0

    def build_header(self, paths, types):
        s = ', '.join(paths)
        return [f"udaf_test({s})"], ["DOUBLE"]

    def eval(self, status, col_a, col_b, weight_a=1, weight_b=1):
        status = status + float(weight_a * col_a + weight_b * col_b)
        return status


"""
from udaf_test import *
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"],[1,2],[1,3]]
kwargs = {}
test = UDAFTest()
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
REGISTER UDAF PYTHON TASK "UDAFTest" IN "udf_funcs\\python_scripts\\udaf_test.py" AS "udaf_test";

select udaf_test(a,b) from test;
select udaf_test(a,b,2) from test;
select udaf_test(a,b,2,3) from test;
select udaf_test(a,b,weight_a=2) from test;
select udaf_test(a,b,weight_b=3) from test;
select udaf_test(a,b,weight_a=2, weight_b=3) from test;

ResultSets:
+-------------------------+
|udaf_test(test.a, test.b)|
+-------------------------+
|                     21.0|
+-------------------------+
Total line number = 1
"""
