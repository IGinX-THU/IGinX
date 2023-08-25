package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.math.BigDecimal;
import java.util.Objects;
import org.bson.*;
import org.bson.types.Decimal128;

public class MongoPoint {
  private final DataType type;
  private final Object value;

  public MongoPoint(DataType type, Object value) {
    this.type = type;
    this.value = value;
  }

  public MongoPoint(DataType type) {
    this.type = type;
    switch (this.type) {
      case BOOLEAN:
        this.value = Boolean.FALSE;
        break;
      case INTEGER:
        this.value = 0;
        break;
      case LONG:
        this.value = 0L;
        break;
      case FLOAT:
        this.value = (float) 0;
        break;
      case DOUBLE:
        this.value = (double) 0;
        break;
      case BINARY:
        this.value = new byte[0];
        break;
      default:
        throw new IllegalArgumentException("unknown data type: " + type);
    }
  }

  public static MongoPoint of(BsonValue value) {
    switch (value.getBsonType()) {
      case DOUBLE:
        return new MongoPoint(DataType.FLOAT, (float) ((BsonDouble) value).getValue());
      case STRING:
        return new MongoPoint(DataType.BINARY, ((BsonString) value).getValue().getBytes());
      case BOOLEAN:
        return new MongoPoint(DataType.BOOLEAN, ((BsonBoolean) value).getValue());
      case INT32:
        return new MongoPoint(DataType.INTEGER, ((BsonInt32) value).getValue());
      case INT64:
        return new MongoPoint(DataType.LONG, ((BsonInt64) value).getValue());
      case DECIMAL128:
        return new MongoPoint(DataType.DOUBLE, ((BsonDecimal128) value).doubleValue());
      default:
        throw new IllegalArgumentException("unexpected type");
    }
  }

  public DataType getType() {
    return type;
  }

  public Object getValue() {
    return value;
  }

  public BsonValue getBsonValue() {
    switch (this.type) {
      case BOOLEAN:
        return new BsonBoolean((Boolean) value);
      case INTEGER:
        return new BsonInt32((Integer) value);
      case LONG:
        return new BsonInt64((Long) value);
      case FLOAT:
        return new BsonDouble((Float) value);
      case DOUBLE:
        return new BsonDecimal128(new Decimal128(BigDecimal.valueOf((Double) value)));
      case BINARY:
        return new BsonString(new String((byte[]) value));
      default:
        throw new IllegalArgumentException("unknown data type: " + type);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MongoPoint that = (MongoPoint) o;
    return type == that.type && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
  }
}
