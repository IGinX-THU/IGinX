from iginx_udf import UDTFinDF


class UDFColumnExpand(UDTFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        ori_cols = list(data)
        for i, col_name in enumerate(ori_cols):
            new_pos = 3 * i + 1
            add_column = data[col_name] + 1.5
            mul_column = data[col_name] * 2
            data.insert(loc=new_pos, column=f'{self.udf_name}({col_name}+1.5)', value=add_column)
            data.insert(loc=new_pos+1, column=f'{self.udf_name}({col_name}*2)', value=mul_column)
        return data


    # def __init__(self):
    #     pass
    #
    # def transform(self, data, args, kvargs):
    #     res = self.buildHeader(data)
    #     newRow = []
    #     for num in data[2][1:]:
    #         newRow.append(num)
    #         newRow.append(num + 1.5)
    #         newRow.append(num * 2)
    #     res.append(newRow)
    #     return res
    #
    # def buildHeader(self, data):
    #     colNames = []
    #     colTypes = []
    #     for i in range(1, len(data[0])):
    #         colNames.append("column_expand(" + data[0][i] + ")")
    #         colTypes.append(data[1][i])
    #         colNames.append("column_expand(" + data[0][i] + "+1.5)")
    #         colTypes.append("DOUBLE")
    #         colNames.append("column_expand(" + data[0][i] + "*2)")
    #         colTypes.append(data[1][i])
    #
    #     return [colNames, colTypes]

"""
from udtf_column_expand import UDFColumnExpand
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFColumnExpand()
test.udf_name = 'column_expand'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""