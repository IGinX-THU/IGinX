class UDFCount:
    def __init__(self):
        pass

    def transform(self, data):
        res = self.buildHeader(data)

        countRow = []
        rows = data[2:]
        for row in zip(*rows):
            count = 0
            for num in row:
                if num is not None:
                    count += 1
            countRow.append(count)
        res.append(countRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for name in data[0]:
            colNames.append("udf_count(" + name + ")")
            colTypes.append("LONG")
        return [colNames, colTypes]
