from iginx_udf import UDTFinDF


class UDFMultiply(UDTFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        name = ', '.join(list(data))
        res = data.prod(axis=1).to_frame(name=f'{self.udf_name}({name})').astype(float)
        return res

    # def __init__(self):
    #   pass
    #
    # def transform(self, data, args, kvargs):
    #   res = self.buildHeader(data)
    #   multiplyRet = 1.0
    #   for num in data[2][1:]:
    #     multiplyRet *= num
    #   res.append([multiplyRet])
    #   return res
    #
    # def buildHeader(self, data):
    #   retName = "multiply("
    #   for name in data[0][1:]:
    #     retName += name + ", "
    #   retName = retName[:-2] + ")"
    #   return [[retName], ["DOUBLE"]]

"""
from udtf_multiply import UDFMultiply
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFMultiply()
test.udf_name = 'multiply'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""