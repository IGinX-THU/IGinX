from iginx_udf import UDSF


class UDFTranspose(UDSF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        index_ = [f'{self.udf_name}({i})' for i in range(data.shape[0])]
        data.index = index_
        data = data.transpose()
        return data

    # def __init__(self):
    #   pass
    #
    # def transform(self, data, args, kvargs):
    #   res = self.buildHeader(data)
    #   for row in data[2:]:
    #     del(row[0])
    #   res.extend(list(map(list, zip(*data[2:]))))
    #   return res
    #
    # def buildHeader(self, data):
    #   colNames = []
    #   types = []
    #   count = 0
    #   for i in range(2, len(data)):
    #     colNames.append("transpose(" + str(count) + ")")
    #     count += 1
    #     types.append(data[1][1])
    #   return [colNames, types]

"""
from udsf_transpose import UDFTranspose
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,20],[1,2,30],[2,3,40]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFTranspose()
test.udf_name = 'transpose'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""