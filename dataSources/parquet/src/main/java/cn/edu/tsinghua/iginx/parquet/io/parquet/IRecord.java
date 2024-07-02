package cn.edu.tsinghua.iginx.parquet.io.parquet;

import java.util.*;
import shaded.iginx.org.apache.parquet.io.api.Binary;

public class IRecord implements Iterable<Map.Entry<Integer, Object>> {
  private final List<Map.Entry<Integer, Object>> values = new ArrayList<>();

  public IRecord add(int field, Object value) {
    values.add(new AbstractMap.SimpleEntry<>(field, value));
    return this;
  }

  public int size() {
    return values.size();
  }

  @Override
  public Iterator<Map.Entry<Integer, Object>> iterator() {
    return new Iterator<Map.Entry<Integer, Object>>() {

      private final Iterator<Map.Entry<Integer, Object>> iterator = values.iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Map.Entry<Integer, Object> next() {
        Map.Entry<Integer, Object> entry = iterator.next();
        if (entry.getValue() instanceof Binary) {
          return new AbstractMap.SimpleEntry<>(
              entry.getKey(), ((Binary) entry.getValue()).getBytes());
        }
        return entry;
      }
    };
  }
}
