class UDFColumnExpand:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        newRow = []
        for num in data[2][1:]:
            newRow.append(num)
            newRow.append(num + 1.5)
            newRow.append(num * 2)
        res.append(newRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for i in range(1, len(data[0])):
            colNames.append("column_expand(" + data[0][i] + ")")
            colTypes.append(data[1][i])
            colNames.append("column_expand(" + data[0][i] + "+1.5)")
            colTypes.append("DOUBLE")
            colNames.append("column_expand(" + data[0][i] + "*2)")
            colTypes.append(data[1][i])

        return [colNames, colTypes]
