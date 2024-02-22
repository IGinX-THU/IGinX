import pandas as pd

from iginx_udf import UDAFinDF


class UDFAvg(UDAFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        columns = list(data)
        res = {}
        for col_name in columns:
            num = data[col_name].mean()
            res[f"{self.udf_name}({col_name})"] = [num]
        return pd.DataFrame(data=res)

    # def transform(self, data, args, kvargs):
    #     res = self.buildHeader(data)
    #
    #     avgRow = []
    #     rows = data[2:]
    #     for row in list(zip(*rows))[1:]:
    #         sum, count = 0, 0
    #         for num in row:
    #             if num is not None:
    #                 sum += num
    #                 count += 1
    #         avgRow.append(sum / count)
    #     res.append(avgRow)
    #     return res
    #
    # def buildHeader(self, data):
    #     colNames = []
    #     colTypes = []
    #     for name in data[0][1:]:
    #         colNames.append("udf_avg(" + name + ")")
    #         colTypes.append("DOUBLE")
    #     return [colNames, colTypes]

"""
from udf_avg import UDFAvg
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFAvg()
test.udf_name = 'avg'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""