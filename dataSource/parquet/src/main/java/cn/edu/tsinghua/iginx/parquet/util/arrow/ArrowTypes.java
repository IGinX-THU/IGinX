package cn.edu.tsinghua.iginx.parquet.util.arrow;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Objects;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class ArrowTypes {
  public static ArrowType of(DataType dataType) {
    return minorTypeOf(dataType).getType();
  }

  public static DataType toIginxType(ArrowType arrowType) {
    Objects.requireNonNull(arrowType, "arrowType");
    switch (arrowType.getTypeID()) {
      case Bool:
        return DataType.BOOLEAN;
      case Int:
        return toIginxType((ArrowType.Int) arrowType);
      case FloatingPoint:
        return toIginxType((ArrowType.FloatingPoint) arrowType);
      case Binary:
        return DataType.BINARY;
      default:
        throw new IllegalArgumentException("Unsupported arrow type: " + arrowType);
    }
  }

  public static DataType toIginxType(ArrowType.Int arrowType) {
    Objects.requireNonNull(arrowType, "arrowType");
    switch (arrowType.getBitWidth()) {
      case 32:
        return DataType.INTEGER;
      case 64:
        return DataType.LONG;
      default:
        throw new IllegalArgumentException("Unsupported arrow type: " + arrowType);
    }
  }

  public static DataType toIginxType(ArrowType.FloatingPoint arrowType) {
    Objects.requireNonNull(arrowType, "arrowType");
    switch (arrowType.getPrecision()) {
      case SINGLE:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      default:
        throw new IllegalArgumentException("Unsupported arrow type: " + arrowType);
    }
  }

  public static Types.MinorType minorTypeOf(DataType dataType) {
    Objects.requireNonNull(dataType, "dataType");
    switch (dataType) {
      case BOOLEAN:
        return Types.MinorType.BIT; // 1-bit
      case INTEGER:
        return Types.MinorType.INT; // 32-bit
      case LONG:
        return Types.MinorType.BIGINT; // 64-bit
      case FLOAT:
        return Types.MinorType.FLOAT4; // 32-bit
      case DOUBLE:
        return Types.MinorType.FLOAT8; // 64-bit
      case BINARY:
        return Types.MinorType.VARBINARY; // variable length
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }
}
