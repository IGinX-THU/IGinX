from iginx_udf import UDSF


class UDFTranspose(UDSF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        index_ = [f'{self.udf_name}({i})' for i in range(data.shape[0])]
        data.index = index_
        data = data.transpose()
        return data
