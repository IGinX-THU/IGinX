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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

@Deprecated
public class Table {

  Map<String, Integer> pathIndexMap = new HashMap<>();

  List<Field> fieldList = new ArrayList<>();

  List<ConcurrentSkipListMap<Long, Object>> columnList = new ArrayList<>();

  public static Table wrap(
      Map<String, DataType> header, Map<String, ConcurrentSkipListMap<Long, Object>> data)
      throws IOException {
    Table table = new Table();
    for (Map.Entry<String, DataType> e : header.entrySet()) {
      table.declareColumn(e.getKey(), e.getValue());
    }
    for (Map.Entry<String, ConcurrentSkipListMap<Long, Object>> e : data.entrySet()) {
      int index = table.pathIndexMap.get(e.getKey());
      table.columnList.set(index, e.getValue());
    }
    return table;
  }

  public int declareColumn(String name, DataType type) {
    Integer index = pathIndexMap.get(name);
    if (index == null) {
      index = fieldList.size();
      pathIndexMap.put(name, index);
      fieldList.add((new Field(name, type)));
      columnList.add(new ConcurrentSkipListMap<>());
    } else {
      DataType oldType = fieldList.get(index).getType();
      if (!oldType.equals(type)) {
        throw new IllegalArgumentException(
            "insert " + type + " into " + name + "(" + oldType + ")");
      }
    }
    return index;
  }

  public List<Field> getHeader() {
    return Collections.unmodifiableList(fieldList);
  }

  public Object get(int field, long key) {
    return columnList.get(field).get(key);
  }

  public Object put(int field, long key, Object value) {
    return columnList.get(field).put(key, value);
  }

  public List<Column> toColumns() {
    List<Column> columns = new ArrayList<>();
    for (int i = 0; i < fieldList.size(); i++) {
      String physicalPath = fieldList.get(i).getName();
      String pathName =
          physicalPath.replaceAll(Constants.PARQUET_SEPARATOR, Constants.IGINX_SEPARATOR);
      Column column = new Column(pathName, physicalPath, fieldList.get(i).getType());
      column.putBatchData(columnList.get(i));
      columns.add(column);
    }
    return columns;
  }

  public static class Point {
    public final long key;
    public final int field;
    public final Object value;

    public Point(long key, int field, Object value) {
      this.key = key;
      this.field = field;
      this.value = value;
    }
  }

  public Iterable<Point> scanRows() {
    return () ->
        new Iterator<Point>() {

          final List<Iterator<Map.Entry<Long, Object>>> iterators = new ArrayList<>();
          final Map.Entry<Long, Object>[] currents = new Map.Entry[columnList.size()];
          final PriorityQueue<Map.Entry<Long, Integer>> queue;

          {
            Comparator<Map.Entry<Long, Integer>> comparator = (Map.Entry.comparingByKey());
            queue = new PriorityQueue<>(comparator.thenComparing(Map.Entry.comparingByValue()));
            for (int i = 0; i < columnList.size(); i++) {
              iterators.add(columnList.get(i).entrySet().iterator());
              stepField(i);
            }
          }

          @Override
          public boolean hasNext() {
            return !queue.isEmpty();
          }

          @Override
          public Point next() {
            if (queue.isEmpty()) {
              throw new NoSuchElementException();
            }
            Map.Entry<Long, Integer> point = queue.poll();
            int field = point.getValue();
            Object value = currents[field].getValue();
            stepField(field);
            return new Point(point.getKey(), field, value);
          }

          private void stepField(int field) {
            if (iterators.get(field).hasNext()) {
              currents[field] = iterators.get(field).next();
              queue.add(new AbstractMap.SimpleEntry<>(currents[field].getKey(), field));
            } else {
              currents[field] = null;
            }
          }
        };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Table that = (Table) o;
    if (!Objects.equals(fieldList, that.fieldList)) return false;
    if (columnList.size() != that.columnList.size()) return false;
    for (int i = 0; i < columnList.size(); i++) {
      if (!fieldList.get(i).getType().equals(DataType.BINARY)) {
        if (!Objects.equals(columnList.get(i), that.columnList.get(i))) return false;
      } else {
        if (columnList.get(i).size() != that.columnList.get(i).size()) return false;
        Iterator<Map.Entry<Long, Object>> selfIterator = columnList.get(i).entrySet().iterator();
        Iterator<Map.Entry<Long, Object>> thatIterator =
            that.columnList.get(i).entrySet().iterator();
        while (selfIterator.hasNext()) {
          Map.Entry<Long, Object> selfEntry = selfIterator.next();
          Map.Entry<Long, Object> thatEntry = thatIterator.next();
          if (!selfEntry.getKey().equals(thatEntry.getKey())) return false;
          if (!Arrays.equals((byte[]) selfEntry.getValue(), (byte[]) thatEntry.getValue()))
            return false;
        }
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldList, columnList);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Table{");
    for (int i = 0; i < fieldList.size(); i++) {
      sb.append(fieldList.get(i).getName())
          .append("(")
          .append(fieldList.get(i).getType())
          .append(")");
      sb.append("={");
      for (Map.Entry<Long, Object> entry : columnList.get(i).entrySet()) {
        sb.append(entry.getKey()).append("=");
        Object v = entry.getValue();
        if (v instanceof byte[]) {
          sb.append(Arrays.toString((byte[]) v));
        } else {
          sb.append(v);
        }
        sb.append(",");
      }
      sb.append("}, ");
    }
    return sb.toString();
  }
}
