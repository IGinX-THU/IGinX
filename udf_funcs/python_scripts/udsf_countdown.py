from datetime import datetime
class UDFCountdown:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)

        newrow = []
        now = datetime.now()
        for row in data[2:]:
            time = datetime.fromtimestamp(row[0]/1000)
            diffTime = now - time
            newrow.append(row[0])
            for i in row[1:]:
                newrow.append(str(diffTime))
            res.append(newrow)
            newrow = []
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for name in data[0]:
            if name == "key":
                colNames.append("key")
                colTypes.append("LONG")
                continue
            colNames.append("udf_countdown(" + name + ")")
            colTypes.append("BINARY")
        return [colNames, colTypes]