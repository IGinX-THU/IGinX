class UDFPow:
  def __init__(self):
    self._n = 1
    pass

  def transform(self, data, args, kvargs):
    n = self._n
    if 'n' in kvargs:
      n = kvargs['n']
    elif len(args) == 1:
      n = args[0]

    res = self.buildHeader(data, n)
    cosRow = []
    for num in data[2][1:]:
      cosRow.append(float(num ** n))
    res.append(cosRow)
    return res

  def buildHeader(self, data, n):
    colNames = []
    colTypes = []
    for name in data[0][1:]:
      colNames.append("pow({col}, {n})".format(col=name, n=n))
      colTypes.append("DOUBLE")
    return [colNames, colTypes]
