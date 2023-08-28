package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Objects;
import org.bson.*;

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
        return new MongoPoint(DataType.DOUBLE, value.asDouble().getValue());
      case STRING:
        return new MongoPoint(DataType.BINARY, value.asString().getValue().getBytes());
      case BINARY:
        return new MongoPoint(DataType.FLOAT, bytesToFloat(value.asBinary().getData()));
      case BOOLEAN:
        return new MongoPoint(DataType.BOOLEAN, value.asBoolean().getValue());
      case INT32:
        return new MongoPoint(DataType.INTEGER, value.asInt32().getValue());
      case INT64:
        return new MongoPoint(DataType.LONG, value.asInt64().getValue());
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
        return new BsonBinary(floatToBytes((Float) value));
      case DOUBLE:
        return new BsonDouble((Double) value);
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

  private static byte[] floatToBytes(float num) {
    int bits = 0;
    if (num != 0.0) {
      bits ^= Float.floatToIntBits(num) ^ 0x80000000;
    }

    byte byte0 = (byte) (bits >>> 24);
    byte byte1 = (byte) ((bits >> 16) & 0xff);
    byte byte2 = (byte) ((bits >> 8) & 0xff);
    byte byte3 = (byte) (bits & 0xff);

    if (byte3 != 0) {
      return new byte[] {byte0, byte1, byte2, byte3};
    } else if (byte2 != 0) {
      return new byte[] {byte0, byte1, byte2};
    } else if (byte1 != 0) {
      return new byte[] {byte0, byte1};
    } else if (byte0 != 0) {
      return new byte[] {byte0};
    } else {
      return new byte[] {};
    }
  }

  private static float bytesToFloat(byte[] bytes) {
    int bits = 0;
    switch (bytes.length) {
      default:
      case 4:
        bits |= (bytes[3] & 0xff);
      case 3:
        bits |= ((bytes[2] & 0xff) << 8);
      case 2:
        bits |= ((bytes[1] & 0xff) << 16);
      case 1:
        bits |= ((bytes[0] & 0xff) << 24);
        break;
      case 0:
        return 0f;
    }

    return Float.intBitsToFloat(bits ^ 0x80000000);
  }
}
