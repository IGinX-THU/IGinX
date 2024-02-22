from iginx_udf import UDTFinDF


class UDFKeyAddOne(UDTFinDF):
    def eval(self, data):
        data['key'] = data['key'] + 1
        data.columns = ['key' if col == 'key' else f'{self.udf_name}({col})' for col in list(data)]
        return data

    # def __init__(self):
    #     pass
    #
    # # key add 1, only for test
    # def transform(self, data, args, kvargs):
    #     res = self.buildHeader(data)
    #     rows = [data[2][0] + 1, data[2][1]]
    #     res.append(rows)
    #     return res
    #
    # def buildHeader(self, data):
    #     colNames = ["key"]
    #     for name in data[0][1:]:
    #         colNames.append("key_add_one(" + name + ")")
    #     return [colNames, data[1]]


"""
from udtf_key_add_one import UDFKeyAddOne
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFKeyAddOne()
test.udf_name = 'key_add_one'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""