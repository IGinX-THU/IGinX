class UDFMin:
    def __init__(self):
        pass

    def transform(self, data):
        res = self.buildHeader(data)

        minRow = []
        rows = data[2:]
        for row in zip(*rows):
            min = None
            for num in row:
                if num is not None:
                    if min is None:
                        min = num
                    elif min > num:
                        min = num
            minRow.append(min)
        res.append(minRow)
        return res

    def buildHeader(self, data):
        colNames = []
        for name in data[0]:
            colNames.append("udf_min(" + name + ")")
        return [colNames, data[1]]
