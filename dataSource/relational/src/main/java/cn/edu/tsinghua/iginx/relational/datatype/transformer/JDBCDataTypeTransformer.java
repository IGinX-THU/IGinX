/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
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
  public DataType fromEngineType(String dataType, String... parameters) {
    String mappedType = typeMappings.getProperty(dataType.toUpperCase());
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
