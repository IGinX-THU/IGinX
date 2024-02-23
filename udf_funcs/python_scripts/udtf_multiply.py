from iginx_udf import UDTFinDF


class UDFMultiply(UDTFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        name = ', '.join(list(data))
        res = data.prod(axis=1).to_frame(name=f'{self.udf_name}({name})').astype(float)
        return res
