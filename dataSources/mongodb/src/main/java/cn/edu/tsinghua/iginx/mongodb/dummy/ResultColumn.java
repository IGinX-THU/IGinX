package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.BsonType;
import org.bson.BsonValue;

class ResultColumn {

  private final DataType type;

  private final Map<Long, Object> data;

  public ResultColumn(DataType type, Map<Long, Object> data) {
    this.type = type;
    this.data = data;
  }

  public DataType getType() {
    return type;
  }

  public Map<Long, Object> getData() {
    return data;
  }

  public static class Builder {

    private final Collection<SimpleImmutableEntry<Long, BsonValue>> values = new ArrayList<>();
    private DataType type;

    public void add(long key, BsonValue value) {
      values.add(new SimpleImmutableEntry<>(key, value));
    }

    ResultColumn build() {
      if (type != null) {
        if (type == DataType.BINARY) {
          return buildAsBinary();
        }
        return buildByConvertToNotBinary(type);
      }
      try {
        return buildByConvert();
      } catch (Exception e) {
        return buildAsBinary();
      }
    }

    private ResultColumn buildByConvertToNotBinary(DataType type) {
      Map<Long, Object> data = new HashMap<>();
      for (SimpleImmutableEntry<Long, BsonValue> bsonValue : values) {
        Object value = TypeUtils.convertToNotBinaryWithIgnore(bsonValue.getValue(), type);
        data.put(bsonValue.getKey(), value);
      }
      return new ResultColumn(type, data);
    }

    private ResultColumn buildByConvert() {
      BsonType bsonType = analysisType();
      DataType type = TypeUtils.convert(bsonType);
      if (type == DataType.BINARY) {
        return buildAsBinary();
      }
      Map<Long, Object> data = new HashMap<>();
      for (SimpleImmutableEntry<Long, BsonValue> bsonValue : values) {
        BsonValue convertedBsonValue = TypeUtils.convertTo(bsonValue.getValue(), bsonType);
        if (convertedBsonValue != null) {
          Object value = TypeUtils.convert(convertedBsonValue);
          data.put(bsonValue.getKey(), value);
        }
      }
      return new ResultColumn(type, data);
    }

    private BsonType analysisType() {
      Map<BsonType, Long> typeNum =
          values.stream()
              .map(Map.Entry::getValue)
              .collect(Collectors.groupingBy(BsonValue::getBsonType, Collectors.counting()));

      BsonType majorType =
          typeNum.entrySet().stream()
              .max(Map.Entry.comparingByValue())
              .orElseThrow(() -> new IllegalArgumentException("can't build empty column!"))
              .getKey();

      BsonType resultType = majorType;
      switch (majorType) {
        case INT32:
          if (typeNum.containsKey(BsonType.INT64)) {
            resultType = BsonType.INT64;
          }
        case INT64:
          if (typeNum.containsKey(BsonType.DOUBLE)) {
            resultType = BsonType.DOUBLE;
          }
      }
      return resultType;
    }

    private ResultColumn buildAsBinary() {
      DataType type = DataType.BINARY;
      Map<Long, Object> data = new HashMap<>();
      for (SimpleImmutableEntry<Long, BsonValue> entry : values) {
        Long key = entry.getKey();
        BsonValue bsonValue = entry.getValue();
        byte[] value = TypeUtils.convertToBinary(bsonValue);
        data.put(key, value);
      }
      return new ResultColumn(type, data);
    }

    public void setType(DataType type) {
      this.type = type;
    }
  }
}
