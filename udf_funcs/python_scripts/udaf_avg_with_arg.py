from iginx_udf import UDAF


class UDFAvgWithArg(UDAF):
    def init_status(self):
        return {
            "value": 0.0,
            "count": 0,
        }

    def build_header(self, paths, types):
        s = ', '.join(paths)
        col = f"{self.udf_name}({s})"
        return [col], ["DOUBLE"]

    def eval(self, status, data, mul_val=2, add_val=5):
        data = float(data * mul_val + add_val)
        status['value'] += data
        status['count'] += 1
        return status

    def final_func(self, status):
        return float(status['value'] / status['count'])
