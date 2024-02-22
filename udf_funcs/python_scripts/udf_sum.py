import pandas as pd

from iginx_udf import UDAFinDF


class UDFSum(UDAFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        columns = list(data)
        res = {}
        for col_name in columns:
            s = data[col_name].sum()
            res[f"{self.udf_name}({col_name})"] = [s]
        return pd.DataFrame(data=res)

    # def __init__(self):
    #     pass
    #
    # def transform(self, data, args, kvargs):
    #     res = self.buildHeader(data)
    #
    #     sumRow = []
    #     rows = data[2:]
    #     for row in list(zip(*rows))[1:]:
    #         sum = 0
    #         for num in row:
    #             if num is not None:
    #                 sum += num
    #         sumRow.append(sum)
    #     res.append(sumRow)
    #     return res
    #
    # def buildHeader(self, data):
    #     colNames = []
    #     for name in data[0][1:]:
    #         colNames.append("udf_sum(" + name + ")")
    #     return [colNames, data[1][1:]]

"""
import pandas as pd
 
# dictionary of lists
dict = {'name': ["aparna", "pankaj", "sudhir", "Geeku"],
        'degree': ["MBA", "BCA", "M.Tech", "MBA"],
        'score': [90, 40, 80, 98]}
 
# creating a dataframe from a dictionary
df = pd.DataFrame(dict)
"""