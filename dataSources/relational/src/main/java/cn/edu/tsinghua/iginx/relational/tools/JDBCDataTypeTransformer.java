package cn.edu.tsinghua.iginx.relational.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JDBCDataTypeTransformer implements IDataTypeTransformer {
  private final Properties typeMappings;

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
    return typeMappings.getProperty(dataType.name(), "TEXT");
  }

  private static Map<String, DataType> dataTypeMap = new HashMap<>();

  static {
    dataTypeMap.put("BOOLEAN", DataType.BOOLEAN);
    dataTypeMap.put("INTEGER", DataType.INTEGER);
    dataTypeMap.put("LONG", DataType.LONG);
    dataTypeMap.put("FLOAT", DataType.FLOAT);
    dataTypeMap.put("DOUBLE", DataType.DOUBLE);
    dataTypeMap.put("BINARY", DataType.BINARY);
  }

  private static DataType str2DataType(String typeStr) {
    return dataTypeMap.getOrDefault(typeStr, DataType.BINARY);
  }
}
