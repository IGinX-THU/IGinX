class UDFAvg:
    def __init__(self):
        pass

    def transform(self, data):
        res = self.buildHeader(data)

        avgRow = []
        rows = data[2:]
        for row in zip(*rows):
            sum, count = 0, 0
            for num in row:
                if num is not None:
                    sum += num
                    count += 1
            avgRow.append(sum / count)
        res.append(avgRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for name in data[0]:
            colNames.append("udf_avg(" + name + ")")
            colTypes.append("DOUBLE")
        return [colNames, colTypes]
