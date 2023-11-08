package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryTable {

  Map<String, Integer> pathIndexMap = new HashMap<>();

  List<String> pathList = new ArrayList<>();

  List<DataType> typeList = new ArrayList<>();

  List<ConcurrentSkipListMap<Long, Object>> columnList = new ArrayList<>();

  public InMemoryTable() {
    this(Collections.emptyList());
  }

  public InMemoryTable(List<Map.Entry<String, DataType>> header) {
    for (Map.Entry<String, DataType> e : header) {
      declareColumn(e.getKey(), e.getValue());
    }
  }

  public static InMemoryTable wrap(
      Map<String, DataType> header, Map<String, ConcurrentSkipListMap<Long, Object>> data)
      throws IOException {
    InMemoryTable table = new InMemoryTable();
    for (Map.Entry<String, DataType> e : header.entrySet()) {
      table.declareColumn(e.getKey(), e.getValue());
    }
    for (Map.Entry<String, ConcurrentSkipListMap<Long, Object>> e : data.entrySet()) {
      int index = table.pathIndexMap.get(e.getKey());
      table.columnList.set(index, e.getValue());
    }
    return table;
  }

  public void declareColumn(String name, DataType type) {
    Integer index = pathIndexMap.get(name);
    if (index == null) {
      pathIndexMap.put(name, pathList.size());
      pathList.add(name);
      typeList.add(type);
      columnList.add(new ConcurrentSkipListMap<>());
    } else {
      DataType oldType = typeList.get(index);
      if (!oldType.equals(type)) {
        throw new IllegalArgumentException(
            "insert " + type + " into " + name + "(" + oldType + ")");
      }
    }
  }

  public List<Map.Entry<String, DataType>> getHeader() {
    List<Map.Entry<String, DataType>> header = new ArrayList<>();
    for (int i = 0; i < pathList.size(); i++) {
      header.add(new AbstractMap.SimpleEntry<>(pathList.get(i), typeList.get(i)));
    }
    return header;
  }

  public int getFieldCount() {
    return pathList.size();
  }

  public Integer getFieldIndex(String name) {
    return pathIndexMap.get(name);
  }

  public DataType getFieldType(int field) {
    return typeList.get(field);
  }

  public String getFieldName(int field) {
    return pathList.get(field);
  }

  public Object get(int field, long key) {
    return columnList.get(field).get(key);
  }

  public Object put(int field, long key, Object value) {
    return columnList.get(field).put(key, value);
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

          final Iterator<Map.Entry<Long, Object>>[] iterators = new Iterator[columnList.size()];
          final Map.Entry<Long, Object>[] currents = new Map.Entry[columnList.size()];
          final PriorityQueue<Map.Entry<Long, Integer>> queue;

          {
            Comparator<Map.Entry<Long, Integer>> comparator = (Map.Entry.comparingByKey());
            queue = new PriorityQueue<>(comparator.thenComparing(Map.Entry.comparingByValue()));
            for (int i = 0; i < columnList.size(); i++) {
              iterators[i] = columnList.get(i).entrySet().iterator();
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
            if (iterators[field].hasNext()) {
              currents[field] = iterators[field].next();
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
    InMemoryTable that = (InMemoryTable) o;
    if (!Objects.equals(pathList, that.pathList) || !Objects.equals(typeList, that.typeList))
      return false;
    if (columnList.size() != that.columnList.size()) return false;
    for (int i = 0; i < columnList.size(); i++) {
      if (!typeList.get(i).equals(DataType.BINARY)) {
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
    return Objects.hash(pathList, typeList, columnList);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", InMemoryTable.class.getSimpleName() + "[", "]")
        .add("pathList=" + pathList)
        .add("typeList=" + typeList)
        .add("columnList=" + columnList)
        .toString();
  }
}
