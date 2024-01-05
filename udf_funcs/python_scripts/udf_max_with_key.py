class UDFMaxWithKey:
    def __init__(self):
        pass

    # only take one column, return max value and its key
    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)

        max = None
        maxKey = None
        for row in data[2:]:
            num = row[1]
            key = row[0]
            if num is not None:
                if max is None:
                    max = num
                    maxKey = key
                elif max < num:
                    max = num
                    maxKey = key
        res.append([maxKey, max])
        return res

    def buildHeader(self, data):
        colNames = ["key"]
        for name in data[0][1:]:
            colNames.append("udf_max_with_key(" + name + ")")
        return [colNames, data[1]]