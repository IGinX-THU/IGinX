import pandas as pd

from iginx_udf import UDAFinDF


class UDFMax(UDAFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        columns = list(data)
        res = {}
        for col_name in columns:
            num = data[col_name].max()
            res[f"{self.udf_name}({col_name})"] = [num]
        return pd.DataFrame(data=res)


    # def __init__(self):
    #     pass
    #
    # def transform(self, data, args, kvargs):
    #     res = self.buildHeader(data)
    #
    #     maxRow = []
    #     rows = data[2:]
    #     for row in list(zip(*rows))[1:]:
    #         max = None
    #         for num in row:
    #             if num is not None:
    #                 if max is None:
    #                     max = num
    #                 elif max < num:
    #                     max = num
    #         maxRow.append(max)
    #     res.append(maxRow)
    #     return res
    #
    # def buildHeader(self, data):
    #     colNames = []
    #     for name in data[0][1:]:
    #         colNames.append("udf_max(" + name + ")")
    #     return [colNames, data[1][1:]]

"""
from udf_max import UDFMax
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFMax()
test.udf_name = 'max'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""