class UDFTranspose:
  def __init__(self):
    pass

  def transform(self, data, args, kvargs):
    res = self.buildHeader(data)
    res.extend(list(map(list, zip(*data[2:]))))
    return res

  def buildHeader(self, data):
    colNames = []
    types = []
    count = 0
    for i in range(2, len(data)):
      colNames.append("transpose(" + str(count) + ")")
      count += 1
      types.append(data[1][0])
    return [colNames, types]
