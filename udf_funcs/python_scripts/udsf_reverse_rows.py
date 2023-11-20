class UDFReverseRows:
  def __init__(self):
    pass

  def transform(self, data, args, kvargs):
    res = self.buildHeader(data)
    res.extend(list(reversed(data[2:])))
    return res

  def buildHeader(self, data):
    colNames = []
    for name in data[0]:
      if name != "key":
        colNames.append("reverse_rows(" + name + ")")
      else:
        colNames.append(name)
    return [colNames, data[1]]
