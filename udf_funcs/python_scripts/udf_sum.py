class UDFSum:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)

        sumRow = []
        rows = data[2:]
        for row in list(zip(*rows))[1:]:
            sum = 0
            for num in row:
                if num is not None:
                    sum += num
            sumRow.append(sum)
        res.append(sumRow)
        return res

    def buildHeader(self, data):
        colNames = []
        for name in data[0][1:]:
            colNames.append("udf_sum(" + name + ")")
        return [colNames, data[1][1:]]
