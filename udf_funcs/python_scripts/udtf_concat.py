from iginx_udf import UDTF


def to_str(obj):
    if isinstance(obj, bytes):
        return str(obj, encoding='utf-8')
    else:
        return str(obj)


class Concat(UDTF):
    def build_header(self, paths, types):
        s = ', '.join(paths)
        return [f"{self.udf_name}({s})"], ["BINARY"]

    def eval(self, a, b='', c="[default1]", d="[default2]"):
        string = "{--" + to_str(a) + to_str(b) + to_str(c) + to_str(d) + "--}"
        return string
