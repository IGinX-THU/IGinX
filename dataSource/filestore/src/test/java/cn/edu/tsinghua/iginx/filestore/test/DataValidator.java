package cn.edu.tsinghua.iginx.filestore.test;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

import java.util.*;
import java.util.stream.Collectors;

public class DataValidator {
  public static List<Row> toList(RowStream stream) throws PhysicalException {
    List<Row> rows = new ArrayList<>();
    while (stream.hasNext()) {
      rows.add(stream.next());
    }
    return rows;
  }

  public static List<Row> withBinaryAsString(List<Row> row) {
    List<Row> rows = new ArrayList<>();
    for (Row r : row) {
      rows.add(withBinaryAsString(r));
    }
    return rows;
  }

  public static Map<String, Object> toMap(Row row) {
    Map<String, Object> map = new HashMap<>();
    Header header = row.getHeader();
    Field keyField = header.getKey();
    if (keyField != null) {
      map.put(keyField.getFullName(), row.getKey());
    }
    List<Field> fields = header.getFields();
    for (int i = 0; i < fields.size(); i++) {
      Object value = row.getValue(i);
      if (value instanceof byte[]) {
        value = new String((byte[]) value);
      }
      map.put(fields.get(i).getFullName(), value);
    }
    return map;
  }

  public static Row withBinaryAsString(Row row) {
    List<Object> values = new ArrayList<>();
    for (Object value : row.getValues()) {
      if (value instanceof byte[]) {
        values.add(new String((byte[]) value));
      } else {
        values.add(value);
      }
    }
    Header header = row.getHeader();
    if (header.getKey() != null) {
      return new Row(row.getHeader(), row.getKey(), values.toArray());
    } else {
      return new Row(row.getHeader(), values.toArray());
    }
  }

  public static Row sort(Row row) {
    List<Field> fields = row.getHeader().getFields();
    fields.sort(Comparator.comparing(Field::getFullName));

    List<Object> values = new ArrayList<>();
    for (Field field : fields) {
      values.add(row.getValue(field));
    }

    Header header = new Header(row.getHeader().getKey(), fields);
    return new Row(header, row.getKey(), values.toArray());
  }

  public static List<Row> normalize(List<Row> rows) {
    return rows.stream().map(DataValidator::sort).map(DataValidator::withBinaryAsString).collect(Collectors.toList());
  }
}
