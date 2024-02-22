import math
from iginx_udf import UDTFinDF


class UDFCos(UDTFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        res = data.applymap(lambda x: math.cos(x))
        res.columns = [f"{self.udf_name}({col})" for col in res.columns]
        return res
    # def __init__(self):
    #     pass
    #
    # def transform(self, data, args, kvargs):
    #     res = self.buildHeader(data)
    #     cosRow = []
    #     for num in data[2][1:]:
    #         cosRow.append(math.cos(num))
    #     res.append(cosRow)
    #     return res
    #
    # def buildHeader(self, data):
    #     colNames = []
    #     colTypes = []
    #     for name in data[0][1:]:
    #         colNames.append("cos(" + name + ")")
    #         colTypes.append("DOUBLE")
    #     return [colNames, colTypes]


"""
from udtf_cos import UDFCos
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFCos()
test.udf_name = 'cos'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""