import pandas as pd

from iginx_udf import UDAFinDF


class UDFMin(UDAFinDF):
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
