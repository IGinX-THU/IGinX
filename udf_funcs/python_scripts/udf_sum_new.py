from iginx_udf import UDAF


class SumNew(UDAF):
    @property
    def status(self):
        pass

    @property
    def udf_name(self):
        return "sum_new"

    def build_header(self, paths, types):
        s = ', '.join(paths)
        return [f"arg_test({s})"], ["BINARY"], False

    def eval(self, status, a, b, c="default"):
        pass