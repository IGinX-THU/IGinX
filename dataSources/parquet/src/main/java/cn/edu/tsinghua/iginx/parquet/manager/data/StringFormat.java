package cn.edu.tsinghua.iginx.parquet.manager.data;

public class StringFormat implements ObjectFormat<String> {
  @Override
  public String format(String value) {
    return value;
  }

  @Override
  public String parse(String source) {
    return source;
  }
}
