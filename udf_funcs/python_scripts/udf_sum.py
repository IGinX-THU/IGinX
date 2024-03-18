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
