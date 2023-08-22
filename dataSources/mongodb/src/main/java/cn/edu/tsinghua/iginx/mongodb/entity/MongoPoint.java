package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.bson.*;

public class MongoPoint {
  private final DataType type;
  private final Object value;

  public MongoPoint(DataType type, Object value) {
    this.type = type;
    this.value = value;
  }

  public DataType getType() {
    return type;
  }

  public Object getValue() {
    return value;
  }

  public BsonValue getBsonValue() {
    switch (type) {
      case BOOLEAN:
        return new BsonBoolean((Boolean) value);
      case INTEGER:
        return new BsonInt32((Integer) value);
      case LONG:
        return new BsonInt64((Long) value);
      case FLOAT:
        return new BsonDouble((Float) value);
      case DOUBLE:
        return new BsonDouble((Double) value);
      case BINARY:
        return new BsonBinary((byte[]) value);
      default:
        throw new IllegalArgumentException("unknown data type: " + type);
    }
  }
}
