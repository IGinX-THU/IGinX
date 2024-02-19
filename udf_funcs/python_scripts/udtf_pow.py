from iginx_udf import UDTFinDF


class UDFPow(UDTFinDF):
  @property
  def udf_name(self):
    return "pow"

  def eval(self, data, n=1):
    data = data.drop(columns=['key'])
    df_squared = data.applymap(lambda x: float(x ** n))
    df_squared.columns = ["pow({col}, {n})".format(col=col, n=n) for col in df_squared.columns]
    df_squared.astype('float32')
    return df_squared
