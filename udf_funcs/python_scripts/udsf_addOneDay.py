from datetime import datetime, timedelta
class UDFAddOneDay:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)

        newrow = []
        for row in data[2:]:
            time = datetime.fromtimestamp(row[0]/1000)
            nextDay = time + timedelta(days=1)
            newrow.append(row[0])
            for i in row[1:]:
                newrow.append(str(nextDay))
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
            colNames.append("udsf_addOneDay(" + name + ")")
            colTypes.append("BINARY")
        return [colNames, colTypes]