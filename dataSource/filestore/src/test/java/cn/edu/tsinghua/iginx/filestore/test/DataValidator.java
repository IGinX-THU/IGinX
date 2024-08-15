/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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

  public static Header sort(Header header) {
    List<Field> fields = new ArrayList<>(header.getFields());
    fields.sort(Comparator.comparing(Field::getFullName));
    return new Header(header.getKey(), fields);
  }

  public static Row sort(Row row) {
    Header header = sort(row.getHeader());

    List<Object> values = new ArrayList<>();
    for (Field field : header.getFields()) {
      values.add(row.getValue(field));
    }

    return new Row(header, row.getKey(), values.toArray());
  }

  public static List<Row> normalize(List<Row> rows) {
    return rows.stream()
        .map(DataValidator::sort)
        .map(DataValidator::withBinaryAsString)
        .collect(Collectors.toList());
  }

  public static Row withStringAsBinary(Row row) {
    List<Object> values = new ArrayList<>();
    for (Object value : row.getValues()) {
      if (value instanceof String) {
        values.add(((String) value).getBytes());
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
}
