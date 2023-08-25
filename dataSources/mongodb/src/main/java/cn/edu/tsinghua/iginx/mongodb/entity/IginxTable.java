package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.util.*;

public class IginxTable {

  private final Header header;
  private final Iterable<Row> rows;

  private IginxTable(Header header, Iterable<Row> rows) {
    this.header = header;
    this.rows = rows;
  }

  public RowStream rowStream() {
    return new RowStream() {
      private final Iterator<Row> itr = IginxTable.this.rows.iterator();
      private final Header header = IginxTable.this.header;

      @Override
      public Header getHeader() {
        return header;
      }

      @Override
      public void close() {}

      @Override
      public boolean hasNext() {
        return this.itr.hasNext();
      }

      @Override
      public Row next() {
        return this.itr.next();
      }
    };
  }

  public static class Builder {
    private final TreeMap<Long, ArrayList<Object>> valueTable = new TreeMap<>();
    private final HashMap<String, Integer> fieldIndexes = new HashMap<>();
    private final HashMap<String, Field> fields = new HashMap<>();

    public void append(MongoRow row) {
      MongoId id = row.getId();
      for (Map.Entry<String, MongoPoint> mongoField : row.getFields().entrySet()) {
        MongoPoint point = mongoField.getValue();
        Field field = new Field(mongoField.getKey(), point.getType(), id.getTags());
        int idx =
            fieldIndexes.computeIfAbsent(
                field.getFullName(),
                k -> {
                  fields.put(field.getFullName(), field);
                  return fieldIndexes.size();
                });
        List<Object> values =
            valueTable.computeIfAbsent(id.getKey(), k -> new ArrayList<>(fieldIndexes.size()));
        ensureSizeList(values, idx + 1).set(idx, point.getValue());
      }
    }

    public IginxTable build() {
      List<Field> fieldList = ensureSizeList(new ArrayList<>(), fields.size());
      for (Map.Entry<String, Integer> fieldIndex : this.fieldIndexes.entrySet()) {
        fieldList.set(fieldIndex.getValue(), fields.get(fieldIndex.getKey()));
      }
      Header header = new Header(Field.KEY, fieldList);

      List<Row> rowList = new ArrayList<>(valueTable.size());
      for (Map.Entry<Long, ArrayList<Object>> valueRow : valueTable.entrySet()) {
        List<Object> valueList = ensureSizeList(valueRow.getValue(), header.getFieldSize());
        if (valueRow.getKey() != MongoId.PLACE_HOLDER_KEY) {
          rowList.add(new Row(header, valueRow.getKey(), valueList.toArray()));
        }
      }

      return new IginxTable(header, rowList);
    }

    private static <E> List<E> ensureSizeList(List<E> list, int size) {
      while (list.size() < size) {
        list.add(null);
      }
      return list;
    }
  }
}
