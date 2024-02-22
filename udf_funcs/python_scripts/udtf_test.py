from iginx_udf import UDTF


def to_str(obj):
    if isinstance(obj, bytes):
        return str(obj, encoding='utf-8')
    else:
        return str(obj)


class UDTFTest(UDTF):
    def build_header(self, paths, types):
        s = ', '.join(paths)
        return [f"{self.udf_name}({s})"], ["BINARY"], False

    def eval(self, a, b, c="default"):
        string = "[" + to_str(a) + to_str(b) + to_str(c) + "]"
        print(string)
        return string


"""
from udtf_test import UDTFTestSec
data = [["key","a"],["LONG","INTEGER"],[0,1]]
pos_args = [[0,"a"],[1,2]]
kwargs = {}
test = UDTFTestSec()
test.transform(data,pos_args,kwargs)

insert into test(key,a) values(0,1),(1,2);
insert into test(key,a,b) values(0,1,2),(1,2,3);
REGISTER UDTF PYTHON TASK "ArgTest" IN "E:\\IGinX_Lab\\local\\IGinX\\udf_funcs\\python_scripts\\udtf_test.py" AS "arg_test";
REGISTER UDTF PYTHON TASK "ArgTest" IN "udf_funcs\\python_scripts\\udtf_test.py" AS "arg_test";
REGISTER UDTF PYTHON TASK "ArgTest" IN "udf_funcs\\python_scripts\\udf_arg_test_new.py" AS "arg_test_new";

select arg_test(a,2,c="we") from test;
select arg_test(a,2) from test;
select arg_test(a,"do") from test;
select arg_test(2,a) from test;
select arg_test("do",a) from test;
select arg_test(a,b) from test;

select arg_test_new(a,2,c="we") from test;
select arg_test_new(a,2) from test;
select arg_test_new(a,"do") from test;
select arg_test_new(2,a) from test;
select arg_test_new("do",a) from test;
"""
