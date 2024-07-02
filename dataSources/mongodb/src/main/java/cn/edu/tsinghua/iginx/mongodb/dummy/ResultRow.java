package cn.edu.tsinghua.iginx.mongodb.dummy;

import static java.util.AbstractMap.SimpleImmutableEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.bson.BsonValue;

class ResultRow {

  private final Collection<SimpleImmutableEntry<List<String>, BsonValue>> fields =
      new ArrayList<>();

  private final Collection<SimpleImmutableEntry<List<String>, List<ResultRow>>> arrays =
      new ArrayList<>();

  public void add(List<String> path, BsonValue value) {
    fields.add(new SimpleImmutableEntry<>(path, value));
  }

  public void add(List<String> path, List<ResultRow> rowArray) {
    arrays.add(new SimpleImmutableEntry<>(path, rowArray));
  }

  public int fillInto(
      Map<List<String>, ResultColumn.Builder> builders, long index, List<String> prefix) {
    int maxArraySize = fillArraysInto(builders, index, prefix);
    int insertedRowsNum = Integer.max(maxArraySize, 1);
    fillFieldsInto(builders, index, prefix, 1);

    return insertedRowsNum;
  }

  private void fillFieldsInto(
      Map<List<String>, ResultColumn.Builder> builders,
      long keyBase,
      List<String> prefix,
      int repeat) {
    for (SimpleImmutableEntry<List<String>, BsonValue> field : fields) {
      List<String> suffix = field.getKey();
      prefix.addAll(suffix);
      ResultColumn.Builder builder =
          builders.computeIfAbsent(new ArrayList<>(prefix), k -> new ResultColumn.Builder());
      for (int i = 0; i < suffix.size(); i++) {
        prefix.remove(prefix.size() - 1);
      }

      BsonValue value = field.getValue();
      for (int i = 0; i < repeat; i++) {
        builder.add(keyBase + i, value);
      }
    }
  }

  private int fillArraysInto(
      Map<List<String>, ResultColumn.Builder> builders, long index, List<String> prefix) {
    int maxSize = 0;
    for (SimpleImmutableEntry<List<String>, List<ResultRow>> array : arrays) {
      List<String> suffix = array.getKey();
      prefix.addAll(suffix);

      List<ResultRow> rows = array.getValue();
      for (int offset = 0; offset < rows.size(); offset++) {
        ResultRow row = rows.get(offset);
        int num = row.fillInto(builders, index + offset, prefix);
        if (num > 1) {
          throw new IllegalStateException("Unreachable!");
        }
      }
      maxSize = Integer.max(maxSize, rows.size());

      for (int i = 0; i < suffix.size(); i++) {
        prefix.remove(prefix.size() - 1);
      }
    }
    return maxSize;
  }
}
