from datetime import datetime
class UDFExtractYear:
    def __init__(self):
        pass

    def extractYear(self, num):
        # Unix timestamp is in milliseconds
        timestamp_in_seconds = num / 1000
        dt = datetime.utcfromtimestamp(timestamp_in_seconds)
        return float(dt.year)
    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        dateRow = []
        for num in data[2][1:]:
            dateRow.append(self.extractYear(num))
        res.append(dateRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for name in data[0][1:]:
            colNames.append("cos(" + name + ")")
            colTypes.append("DOUBLE")
        return [colNames, colTypes]
