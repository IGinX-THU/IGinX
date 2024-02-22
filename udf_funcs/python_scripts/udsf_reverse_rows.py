from iginx_udf import UDSF


class UDFReverseRows(UDSF):
    def eval(self, data):
        res = data.iloc[::-1]
        res.columns = ['key' if col == 'key' else f'{self.udf_name}({col})' for col in data.columns]
        return res

    # def __init__(self):
    #   pass
    #
    # def transform(self, data, args, kvargs):
    #   res = self.buildHeader(data)
    #   res.extend(list(reversed(data[2:])))
    #   return res
    #
    # def buildHeader(self, data):
    #   colNames = []
    #   for name in data[0]:
    #     if name != "key":
    #       colNames.append("reverse_rows(" + name + ")")
    #     else:
    #       colNames.append(name)
    #   return [colNames, data[1]]

"""
from udsf_reverse_rows import UDFReverseRows
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFReverseRows()
test.udf_name = 'reverse_rows'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""