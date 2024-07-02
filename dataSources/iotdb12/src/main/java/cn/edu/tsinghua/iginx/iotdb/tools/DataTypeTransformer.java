package cn.edu.tsinghua.iginx.iotdb.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DataTypeTransformer {

  public static final String BOOLEAN = "BOOLEAN";
  public static final String FLOAT = "FLOAT";
  public static final String DOUBLE = "DOUBLE";
  public static final String INT32 = "INT32";
  public static final String INT64 = "INT64";
  public static final String TEXT = "TEXT";

  public static TSDataType strToIoTDB(String dataType) {
    switch (dataType) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case INT32:
        return TSDataType.INT32;
      case INT64:
        return TSDataType.INT64;
      case TEXT:
        return TSDataType.TEXT;
      default:
        break;
    }
    return null;
  }

  public static DataType strFromIoTDB(String dataType) {
    switch (dataType) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case TEXT:
        return DataType.BINARY;
      default:
        break;
    }
    return null;
  }

  public static TSDataType toIoTDB(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case INTEGER:
        return TSDataType.INT32;
      case LONG:
        return TSDataType.INT64;
      case BINARY:
        return TSDataType.TEXT;
      default:
        break;
    }
    return null;
  }
}
