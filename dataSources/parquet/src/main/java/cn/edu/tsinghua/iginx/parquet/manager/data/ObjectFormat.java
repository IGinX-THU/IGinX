package cn.edu.tsinghua.iginx.parquet.manager.data;

public interface ObjectFormat<V> {

  String format(V value);

  V parse(String source);
}
