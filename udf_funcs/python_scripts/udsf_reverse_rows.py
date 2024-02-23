from iginx_udf import UDSF


class UDFReverseRows(UDSF):
    def eval(self, data):
        res = data.iloc[::-1]
        res.columns = ['key' if col == 'key' else f'{self.udf_name}({col})' for col in data.columns]
        return res
