class UDFReverseRows:
  def __init__(self):
    pass

  def transform(self, rows):
    # header = []
    # for i in range(len(rows[0])):
    #   if type(rows[0][i]) == 'int':
    #     pass
    #   header.append("tp: {}".format(type(rows[0][i])))
    # rows.append(header)
    #return list(reversed(rows))
    ans = [rows[0], rows[1]]
    for i in range(len(rows) -1, 1, -1):
        ans.append(rows[i])
    return ans
