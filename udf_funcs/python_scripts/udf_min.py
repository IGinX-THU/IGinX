import pandas as pd

from iginx_udf import UDAFinDF


class UDFMin(UDAFinDF):
    # def __init__(self):
    #     super().__init__()

    def eval(self, data):
        data = data.drop(columns=['key'])
        columns = list(data)
        res = {}
        udf_name = self.udf_name
        for col_name in columns:
            col = data[col_name]
            min_val = col.min()
            res[f"{udf_name}({col_name})"] = [min_val]
        return pd.DataFrame(data=res)

    # def transform(self, data, args, kvargs):
    #     res = self.buildHeader(data)
    #
    #     minRow = []
    #     rows = data[2:]
    #     for row in list(zip(*rows))[1:]:
    #         min = None
    #         for num in row:
    #             if num is not None:
    #                 if min is None:
    #                     min = num
    #                 elif min > num:
    #                     min = num
    #         minRow.append(min)
    #     res.append(minRow)
    #     return res
    #
    # def buildHeader(self, data):
    #     colNames = []
    #     for name in data[0][1:]:
    #         colNames.append("udf_min(" + name + ")")
    #     return [colNames, data[1][1:]]

"""
from udf_min import UDFMin
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFMin()
test.udf_name = 'min'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""