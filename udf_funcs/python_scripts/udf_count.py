import pandas as pd

from iginx_udf import UDAFinDF


class UDFCount(UDAFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        columns = list(data)
        res = {}
        for col_name in columns:
            num = data[col_name].count()
            res[f"{self.udf_name}({col_name})"] = [num]
        return pd.DataFrame(data=res)
