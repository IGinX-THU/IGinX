import math


class UDFCos:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        cosRow = []
        for num in data[2][1:]:
            cosRow.append(math.cos(num))
        res.append(cosRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for name in data[0][1:]:
            colNames.append("cos(" + name + ")")
            colTypes.append("DOUBLE")
        return [colNames, colTypes]
