package cn.edu.tsinghua.iginx.parquet.io;

import java.util.*;

public class IginxRecord implements Iterable<Map.Entry<Integer, Object>> {
  private final List<Map.Entry<Integer, Object>> values = new ArrayList<>();

  public void add(int field, Object value) {
    values.add(new AbstractMap.SimpleEntry<>(field, value));
  }

  public int size() {
    return values.size();
  }

  @Override
  public Iterator<Map.Entry<Integer, Object>> iterator() {
    return values.iterator();
  }
}
