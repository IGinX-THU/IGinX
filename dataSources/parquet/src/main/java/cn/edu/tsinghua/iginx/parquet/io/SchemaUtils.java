package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class SchemaUtils {
  public static final String KEY_FIELD_NAME = "key";

  public static MessageType getMessageTypeStartWithKey(
      String name, List<Map.Entry<String, DataType>> header) {
    List<Type> fields = new ArrayList<>();
    fields.add(getParquetType(KEY_FIELD_NAME, DataType.LONG));
    for (Map.Entry<String, DataType> entry : header) {
      fields.add(getParquetType(entry.getKey(), entry.getValue()));
    }
    return new MessageType(name, fields);
  }

  private static PrimitiveType getParquetType(String name, DataType type) {
    switch (type) {
      case BOOLEAN:
        return new PrimitiveType(
            Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, name);
      case INTEGER:
        return new PrimitiveType(
            Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, name);
      case LONG:
        return new PrimitiveType(
            Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, name);
      case FLOAT:
        return new PrimitiveType(
            Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, name);
      case DOUBLE:
        return new PrimitiveType(
            Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, name);
      case BINARY:
        return new PrimitiveType(
            Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, name);
      default:
        throw new RuntimeException("Unsupported data type: " + type);
    }
  }

  public static DataType getIginxType(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
    switch (primitiveTypeName) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BINARY:
        return DataType.BINARY;
      default:
        throw new RuntimeException("Unsupported data type: " + primitiveTypeName);
    }
  }

  public static Integer getFieldIndex(MessageType schema, String fieldName) {
    try {
      return schema.getFieldIndex(fieldName);
    } catch (InvalidRecordException e) {
      return null;
    }
  }
}
