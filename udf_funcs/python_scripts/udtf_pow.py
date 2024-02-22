from iginx_udf import UDTFinDF


class UDFPow(UDTFinDF):
  def eval(self, data, n=1):
    data = data.drop(columns=['key'])
    df_squared = data.applymap(lambda x: float(x ** n))
    df_squared.columns = ["pow({col}, {n})".format(col=col, n=n) for col in df_squared.columns]
    df_squared.astype('float32')
    return df_squared


"""
from udtf_pow import UDFPow


data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,2,3]]
pos_args = [[0,"a"],[0,"b"],[1,3]]
kwargs = {}
test = UDFPow()
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
SELECT pow(a, 2), pow(b, n=2) FROM test;
"""