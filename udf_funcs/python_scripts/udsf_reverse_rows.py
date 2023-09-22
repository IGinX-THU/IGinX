class UDFReverseRows:
  def __init__(self):
    pass

  def transform(self, data):
    res = self.buildHeader(data)
    res.extend(list(reversed(data[2:])))
    return res

  def buildHeader(self, data):
    colNames = []
    for name in data[0]:
      colNames.append("reverse_rows(" + name + ")")
    return [colNames, data[1]]
