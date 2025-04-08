package cn.edu.tsinghua.iginx.relational.datatype.transformer;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.sql.Types;

public class OracleDataTypeTransformer implements IDataTypeTransformer {
  private static final OracleDataTypeTransformer INSTANCE = new OracleDataTypeTransformer();

  public static OracleDataTypeTransformer getInstance() {
    return INSTANCE;
  }

  @Override
  public DataType fromEngineType(int type, String dataType, int precision, int scale) {
    switch (type) {
      case Types.NUMERIC:
        if (scale == 0) {
          if (precision <= 1) {
            return BOOLEAN;
          } else if (precision <= 10) {
            return INTEGER;
          } else if (precision <= 19) {
            return LONG;
          }
        }
        return DOUBLE;
      case Types.VARCHAR:
        return BINARY;
      default:
        switch (dataType.toUpperCase()) {
          case "BINARY_FLOAT":
            return FLOAT;
          case "BINARY_DOUBLE":
            return DOUBLE;
          default:
            return BINARY;
        }
    }
  }

  public String toEngineType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return "NUMBER(1)";
      case INTEGER:
        return "NUMBER(10)";
      case LONG:
        return "NUMBER(19)";
      case FLOAT:
        return "BINARY_FLOAT";
      case DOUBLE:
        return "BINARY_DOUBLE";
      case BINARY:
        return "VARCHAR2(4000)";
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }
}
