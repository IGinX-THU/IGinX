package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.bson.BsonType;

public class TypeUtils {
  public static DataType getType(BsonType type) {
    switch (type) {
      case DOUBLE:
        return DataType.DOUBLE;
      case BINARY:
        return DataType.BINARY;
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      default:
        throw new IllegalArgumentException("unexpected type");
    }
  }
}
