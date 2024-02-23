from iginx_udf import UDAF


class UDFMaxWithKey(UDAF):
    def init_status(self):
        return [-1, 0]

    def build_header(self, paths, types):
        col = f"{self.udf_name}({paths[0]})"
        type = types[0]
        print(col, type)
        return ['key', col], ['LONG', type]

    def eval(self, status, data):
        if status[0] == -1:
            status[1] = data
            status[0] = self.get_key()
        else:
            if status[1] < data:
                status[1] = data
                status[0] = self.get_key()
        return status
