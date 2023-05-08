class UDFMultiply:
  def __init__(self):
    pass

  def transform(self, data):
    res = self.buildHeader(data)
    multiplyRet = 1.0
    for num in data[2]:
      multiplyRet *= num
    res.append([multiplyRet])
    return res

  def buildHeader(self, data):
    retName = "multiply("
    for name in data[0]:
      retName += name + ", "
    retName = retName[:-2] + ")"
    return [[retName], ["DOUBLE"]]
