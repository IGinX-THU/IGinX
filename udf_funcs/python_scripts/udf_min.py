import pandas as pd

from iginx_udf import UDAFinDF


class UDFMin(UDAFinDF):
    # def __init__(self):
    #     super().__init__()

    def eval(self, data):
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
