from udf_funcs.iginx_udf import UDTF


class ArgTest(UDTF):
    @property
    def udf_name(self):
        return "arg_test"

    def build_df_with_header(self, paths, types, with_key=False):
        return ["arg_test"], ["BINARY"], False

    def eval(self, a, b, c="cccc"):
        string = "[" + str(a) + str(b) + str(c) + "]"
        print(string)
        return string,

# from udf_funcs.iginx_udf import *
#
#
# data = [["key","a"],["LONG","INTEGER"],[0,1]]
# pos_args = [[0,"a"],[1,2]]
# kwargs = {"c":"aa"}
# test = ArgTest()
# test.udf_process(data,pos_args,kwargs)

# insert into test(key,a) values(0,1),(1,2);
# REGISTER UDTF PYTHON TASK "ArgTest" IN "E:\\IGinX_Lab\\local\\IGinX\\udf_funcs\\python_scripts\\udf_arg_test.py" AS "arg_test";
# REGISTER UDTF PYTHON TASK "ArgTest" IN "udf_funcs\\python_scripts\\udf_arg_test_new.py" AS "arg_test_new";

# select arg_test(a,2,c="we") from test;
# select arg_test(a,2) from test;
# select arg_test(a,"do") from test;
# select arg_test(2,a) from test;
# select arg_test("do",a) from test;

# select arg_test_new(a,2,c="we") from test;
# select arg_test_new(a,2) from test;
# select arg_test_new(a,"do") from test;
# select arg_test_new(2,a) from test;
# select arg_test_new("do",a) from test;
