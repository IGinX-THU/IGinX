class UDFKeyAddOne:
    def __init__(self):
        pass

    # key add 1, only for test
    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        rows = [data[2][0] + 1, data[2][1]]
        res.append(rows)
        return res

    def buildHeader(self, data):
        colNames = ["key"]
        for name in data[0][1:]:
            colNames.append("key_add_one(" + name + ")")
        return [colNames, data[1]]