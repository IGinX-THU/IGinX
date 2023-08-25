package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import java.util.stream.Collectors;

public class MongoTable implements Iterable<MongoRow> {

  private final DataView dataView;

  public MongoTable(DataView dataView) {
    this.dataView = dataView;
  }

  public Set<MongoRow> headers() {
    return mergeRows(getHeaders(this.dataView)).stream()
        .map(
            row ->
                new MongoRow(
                    row.getId(),
                    row.getFields().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey, e -> new MongoPoint(e.getValue().getType())))))
        .collect(Collectors.toSet());
  }

  @Override
  public Iterator<MongoRow> iterator() {
    if (this.dataView.isColumnData()) {
      return new MongoPointIterator(this.dataView);
    } else {
      return new MongoRowIterator(this.dataView);
    }
  }

  private static class MongoPointIterator implements Iterator<MongoRow> {
    private final DataView dataView;
    private final List<MongoRow> headers;
    private int pathIndex;
    private Iterator<MongoRow> pointIterator;

    public MongoPointIterator(DataView dataView) {
      assert dataView.isColumnData();
      this.dataView = dataView;
      this.headers = getHeaders(dataView);
      this.pathIndex = -1;
      this.pointIterator = null;
    }

    @Override
    public boolean hasNext() {
      while (pathIndex < this.dataView.getPathNum()) {
        if (pointIterator == null || !pointIterator.hasNext()) {
          pointIterator = nextPointIterator();
        } else {
          return true;
        }
      }
      return false;
    }

    @Override
    public MongoRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return pointIterator.next();
    }

    private Iterator<MongoRow> nextPointIterator() {
      pathIndex++;
      if (pathIndex >= this.dataView.getPathNum()) {
        return null;
      }

      MongoRow header = headers.get(pathIndex);
      Map<String, String> tags = header.getId().getTags();
      String pathName = header.getFields().keySet().iterator().next();
      DataType type = header.getFields().values().iterator().next().getType();
      BitmapView columnBitmap = dataView.getBitmapView(pathIndex);

      List<MongoRow> pointList = new ArrayList<>();
      for (int keyIndex = 0, cnt = 0; keyIndex < dataView.getKeySize(); keyIndex++) {
        if (columnBitmap.get(keyIndex)) {
          Map<String, MongoPoint> fields = new TreeMap<>();
          Object value = dataView.getValue(pathIndex, cnt++);
          fields.put(pathName, new MongoPoint(type, value));

          long key = dataView.getKey(keyIndex);
          pointList.add(new MongoRow(new MongoId(key, tags), fields));
        }
      }
      return pointList.iterator();
    }
  }

  private static class MongoRowIterator implements Iterator<MongoRow> {
    private final DataView dataView;
    private final List<MongoRow> headers;
    private int keyIndex;
    private Iterator<MongoRow> rowIterator;

    public MongoRowIterator(DataView dataView) {
      assert dataView.isRowData();
      this.dataView = dataView;
      this.headers = mergeRows(getHeaders(dataView));
      this.keyIndex = -1;
      this.rowIterator = null;
    }

    @Override
    public boolean hasNext() {
      while (keyIndex < this.dataView.getKeySize()) {
        if (rowIterator == null || !rowIterator.hasNext()) {
          rowIterator = nextTowIterator();
        } else {
          return true;
        }
      }
      return false;
    }

    @Override
    public MongoRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return rowIterator.next();
    }

    private Iterator<MongoRow> nextTowIterator() {
      keyIndex++;
      if (keyIndex >= this.dataView.getKeySize()) {
        return null;
      }

      long key = dataView.getKey(keyIndex);
      ArrayList<Object> rowValues = getRowValues(dataView, keyIndex);
      List<MongoRow> rowList = new ArrayList<>();
      for (MongoRow header : headers) {
        Map<String, MongoPoint> fields = getRowFields(header.getFields(), rowValues);
        if (!fields.isEmpty()) {
          Map<String, String> tags = header.getId().getTags();
          rowList.add(new MongoRow(new MongoId(key, tags), fields));
        }
      }
      return rowList.iterator();
    }

    private static ArrayList<Object> getRowValues(DataView dataView, int keyIndex) {
      BitmapView bitmapView = dataView.getBitmapView(keyIndex);
      ArrayList<Object> values = new ArrayList<>(bitmapView.getSize());
      for (int i = 0, cnt = 0; i < bitmapView.getSize(); i++) {
        if (bitmapView.get(i)) {
          values.add(dataView.getValue(keyIndex, cnt++));
        } else {
          values.add(null);
        }
      }
      return values;
    }

    private static Map<String, MongoPoint> getRowFields(
        Map<String, MongoPoint> headerFields, ArrayList<Object> rowValues) {
      Map<String, MongoPoint> fields = new TreeMap<>();
      for (Map.Entry<String, MongoPoint> fieldTemplate : headerFields.entrySet()) {
        MongoPoint pointTemplate = fieldTemplate.getValue();
        int pathIndex = (Integer) pointTemplate.getValue();
        Object value = rowValues.get(pathIndex);
        if (value != null) {
          String path = fieldTemplate.getKey();
          MongoPoint point = new MongoPoint(pointTemplate.getType(), value);
          fields.put(path, point);
        }
      }
      return fields;
    }
  }

  private static List<MongoRow> mergeRows(List<MongoRow> headers) {
    return headers.stream()
        .collect(Collectors.groupingBy(MongoRow::getId))
        .entrySet()
        .stream()
        .map(
            group -> {
              MongoId groupId = group.getKey();
              List<MongoRow> groupFields = group.getValue();
              Map<String, MongoPoint> fields = new HashMap<>();
              for (MongoRow field : groupFields) {
                fields.putAll(field.getFields());
              }
              return new MongoRow(groupId, fields);
            })
        .collect(Collectors.toList());
  }

  private static List<MongoRow> getHeaders(DataView dataView) {
    int pathNum = dataView.getPathNum();
    List<String> pathList = dataView.getPaths();
    List<Map<String, String>> tagsList = dataView.getTagsList();
    List<DataType> typeList = dataView.getDataTypeList();

    List<MongoRow> rows = new ArrayList<>(dataView.getPathNum());
    for (int pathIdx = 0; pathIdx < pathNum; pathIdx++) {
      Map<String, MongoPoint> fields = new TreeMap<>();
      fields.put(pathList.get(pathIdx), new MongoPoint(typeList.get(pathIdx), pathIdx));

      Map<String, String> tags = new TreeMap<>();
      if (tagsList != null && pathIdx < tagsList.size()) {
        Map<String, String> tagsInIdx = tagsList.get(pathIdx);
        if (tagsInIdx != null) {
          tags = tagsInIdx;
        }
      }
      rows.add(new MongoRow(new MongoId(MongoId.PLACE_HOLDER_KEY, tags), fields));
    }
    return rows;
  }
}
