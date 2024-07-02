package cn.edu.tsinghua.iginx.parquet.manager.data;

public class LongFormat implements ObjectFormat<Long> {
  @Override
  public String format(Long value) {
    return value.toString();
  }

  @Override
  public Long parse(String source) {
    return Long.parseLong(source);
  }
}
