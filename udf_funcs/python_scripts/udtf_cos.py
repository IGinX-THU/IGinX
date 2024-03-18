import math
from iginx_udf import UDTFinDF


class UDFCos(UDTFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        res = data.applymap(lambda x: math.cos(x))
        res.columns = [f"{self.udf_name}({col})" for col in res.columns]
        return res
