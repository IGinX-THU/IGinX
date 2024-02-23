from iginx_udf import UDTFinDF


class UDFKeyAddOne(UDTFinDF):
    def eval(self, data):
        data['key'] = data['key'] + 1
        data.columns = ['key' if col == 'key' else f'{self.udf_name}({col})' for col in list(data)]
        return data
