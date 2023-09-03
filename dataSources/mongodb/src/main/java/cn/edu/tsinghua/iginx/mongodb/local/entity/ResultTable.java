package cn.edu.tsinghua.iginx.mongodb.local.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.StringWriter;
import java.util.*;
import org.bson.*;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriter;

public class ResultTable {
  private final List<Field> fields;

  private final Map<Long, SortedMap<Integer, Object>> data;

  private ResultTable(List<Field> fields, Map<Long, SortedMap<Integer, Object>> data) {
    this.fields = fields;
    this.data = data;
  }

  public List<Field> getFields() {
    return fields;
  }

  public Set<Long> keySet() {
    return data.keySet();
  }

  public Map<Integer, Object> getRow(Long key) {
    return data.get(key);
  }

  public static class Builder {
    private final Map<String, Integer> fieldIndexes = new HashMap<>();

    private final Map<Integer, Object> lastValues = new HashMap<>();

    private final Map<Long, SortedMap<Integer, Object>> data = new HashMap<>();

    public ResultTable build() {
      Field[] fields = new Field[fieldIndexes.size()];
      for (Map.Entry<String, Integer> fieldIndex : fieldIndexes.entrySet()) {
        Integer index = fieldIndex.getValue();
        Object lastValue = lastValues.get(index);
        DataType type = getType(lastValue);
        fields[index] = new Field(fieldIndex.getKey(), type);
      }

      return new ResultTable(Arrays.asList(fields), data);
    }

    private DataType getType(Object lastValue) {
      if (lastValue != null) {
        if (lastValue instanceof Double) {
          return DataType.DOUBLE;
        } else if (lastValue instanceof Long) {
          return DataType.LONG;
        } else if (lastValue instanceof Integer) {
          return DataType.INTEGER;
        } else if (lastValue instanceof byte[]) {
          return DataType.BINARY;
        } else if (lastValue instanceof Boolean) {
          return DataType.BOOLEAN;
        }
      }
      return DataType.BINARY;
    }

    private long currentKey = 0;
    private Map<Integer, Object> currentRow = null;

    public void switchKey(long key) {
      this.currentKey = key;
      this.currentRow = null;
    }

    public int put(String field) {
      return fieldIndexes.computeIfAbsent(field, k -> fieldIndexes.size());
    }

    public void put(String field, BsonValue rawValue) {
      int index = put(field);
      Object value = this.getValue(rawValue);
      this.lastValues.put(index, value);
      this.getCurrentRow().put(index, value);
    }

    private Map<Integer, Object> getCurrentRow() {
      if (this.currentRow == null) {
        this.currentRow = this.data.computeIfAbsent(this.currentKey, k -> new TreeMap<>());
      }
      return this.currentRow;
    }

    private Object getValue(BsonValue value) {
      switch (value.getBsonType()) {
        case DOUBLE:
          return ((BsonDouble) value).getValue();
        case STRING:
          return ((BsonString) value).getValue().getBytes();
        case BOOLEAN:
          return ((BsonBoolean) value).getValue();
        case DATE_TIME:
          return ((BsonDateTime) value).getValue();
        case INT32:
          return ((BsonInt32) value).getValue();
        case TIMESTAMP:
          return ((BsonTimestamp) value).getValue();
        case INT64:
          return ((BsonInt64) value).getValue();
      }

      StringWriter writer = new StringWriter();
      new BsonValueCodec().encode(new JsonWriter(writer), value, EncoderContext.builder().build());
      return writer.toString().getBytes();
    }
  }
}
