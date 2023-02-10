class UDFReverseRows:
  def __init__(self):
    pass

  def transform(self, rows):
    return list(reversed(rows))
