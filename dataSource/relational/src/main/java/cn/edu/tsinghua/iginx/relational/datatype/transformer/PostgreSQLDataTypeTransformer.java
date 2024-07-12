/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.relational.datatype.transformer;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class PostgreSQLDataTypeTransformer implements IDataTypeTransformer {

  private static final PostgreSQLDataTypeTransformer INSTANCE = new PostgreSQLDataTypeTransformer();

  public static PostgreSQLDataTypeTransformer getInstance() {
    return INSTANCE;
  }

  public DataType fromEngineType(String dataType) {
    if (dataType.equalsIgnoreCase("bool")) {
      return BOOLEAN;
    } else if (dataType.equalsIgnoreCase("int")
        || dataType.equalsIgnoreCase("int2")
        || dataType.equalsIgnoreCase("int4")
        || dataType.equalsIgnoreCase("serial2")
        || dataType.equalsIgnoreCase("serial4")) {
      return INTEGER;
    } else if (dataType.equalsIgnoreCase("int8") || dataType.equalsIgnoreCase("serial8")) {
      return LONG;
    } else if (dataType.equalsIgnoreCase(("float4"))) {
      return FLOAT;
    } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float8")) {
      return DOUBLE;
    } else {
      return BINARY;
    }
  }

  public String toEngineType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return "BOOLEAN";
      case INTEGER:
        return "INTEGER";
      case LONG:
        return "BIGINT";
      case FLOAT:
        return "REAL";
      case DOUBLE:
        return "DOUBLE PRECISION";
      case BINARY:
      default:
        return "TEXT";
    }
  }
}
