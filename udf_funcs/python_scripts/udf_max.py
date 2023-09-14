class UDFMax:
    def __init__(self):
        pass

    def transform(self, data):
        res = self.buildHeader(data)

        maxRow = []
        rows = data[2:]
        for row in zip(*rows):
            max = None
            for num in row:
                if num is not None:
                    if max is None:
                        max = num
                    elif max < num:
                        max = num
            maxRow.append(max)
        res.append(maxRow)
        return res

    def buildHeader(self, data):
        colNames = []
        for name in data[0]:
            colNames.append("udf_max(" + name + ")")
        return [colNames, data[1]]
