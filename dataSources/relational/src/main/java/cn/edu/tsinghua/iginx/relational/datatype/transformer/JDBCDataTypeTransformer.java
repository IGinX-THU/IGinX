package cn.edu.tsinghua.iginx.relational.datatype.transformer;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JDBCDataTypeTransformer implements IDataTypeTransformer {
  private final Properties typeMappings;

  private static final String prefix = "IGinX-";

  public JDBCDataTypeTransformer(Properties properties) {
    typeMappings = properties;
  }

  @Override
  public DataType fromEngineType(String dataType) {
    String mappedType = typeMappings.getProperty(dataType);
    if (mappedType != null) {
      return str2DataType(mappedType);
    }
    return DataType.BINARY;
  }

  @Override
  public String toEngineType(DataType dataType) {
    return typeMappings.getProperty(prefix + dataType.name(), "TEXT");
  }

  private static final Map<String, DataType> dataTypeMap = new HashMap<>();

  static {
    dataTypeMap.put(prefix + "BOOLEAN", DataType.BOOLEAN);
    dataTypeMap.put(prefix + "INTEGER", DataType.INTEGER);
    dataTypeMap.put(prefix + "LONG", DataType.LONG);
    dataTypeMap.put(prefix + "FLOAT", DataType.FLOAT);
    dataTypeMap.put(prefix + "DOUBLE", DataType.DOUBLE);
    dataTypeMap.put(prefix + "BINARY", DataType.BINARY);
  }

  private static DataType str2DataType(String typeStr) {
    return dataTypeMap.getOrDefault(typeStr, DataType.BINARY);
  }
}
