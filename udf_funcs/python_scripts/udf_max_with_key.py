from iginx_udf import UDAF


class UDFMaxWithKey(UDAF):
    def init_status(self):
        return [-1, 0]

    def build_header(self, paths, types):
        col = f"{self.udf_name}({paths[0]})"
        type = types[0]
        print(col, type)
        return ['key', col], ['LONG', type]

    def eval(self, status, data):
        if status[0] == -1:
            status[1] = data
            status[0] = self.get_key()
        else:
            if status[1] < data:
                status[1] = data
                status[0] = self.get_key()
        return status

    # def __init__(self):
    #     pass
    #
    # # only take one column, return max value and its key
    # def transform(self, data, args, kvargs):
    #     res = self.buildHeader(data)
    #
    #     max = None
    #     maxKey = None
    #     for row in data[2:]:
    #         num = row[1]
    #         key = row[0]
    #         if num is not None:
    #             if max is None:
    #                 max = num
    #                 maxKey = key
    #             elif max < num:
    #                 max = num
    #                 maxKey = key
    #     res.append([maxKey, max])
    #     return res
    #
    # def buildHeader(self, data):
    #     colNames = ["key"]
    #     for name in data[0][1:]:
    #         colNames.append("udf_max_with_key(" + name + ")")
    #     return [colNames, data[1]]

"""
from udf_max_with_key import UDFMaxWithKey
data = [["key","a"],["LONG","INTEGER"],[0,1],[1,2],[2,3]]
pos_args = [[0,"a"]]
kwargs = {}
test = UDFMaxWithKey()
test.udf_name = 'max_key'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""