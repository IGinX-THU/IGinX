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

import static cn.edu.tsinghua.iginx.thrift.DataType.*;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Properties;

public class OracleDataTypeTransformer extends JDBCDataTypeTransformer {
  private static OracleDataTypeTransformer INSTANCE;

  private OracleDataTypeTransformer(Properties properties) {
    super(properties);
  }

  public static synchronized OracleDataTypeTransformer init(Properties properties) {
    if (INSTANCE == null) {
      INSTANCE = new OracleDataTypeTransformer(properties);
    }
    return INSTANCE;
  }

  public static OracleDataTypeTransformer getInstance() {
    if (INSTANCE == null) {
      throw new IllegalStateException("Not initialized, call init(Properties) first");
    }
    return INSTANCE;
  }

  @Override
  public DataType fromEngineType(String dataType, int precision, int scale) {
    switch (dataType.toUpperCase()) {
      case "NUMBER":
        if (scale == 0) {
          if (precision == 0) {
            return LONG;
          } else if (precision <= 1) {
            return BOOLEAN;
          } else if (precision <= 10) {
            return INTEGER;
          } else if (precision <= 19) {
            return LONG;
          }
        }
        return DOUBLE;
      case "FLOAT":
        return DOUBLE;
      case "BINARY_FLOAT":
        return FLOAT;
      case "BINARY_DOUBLE":
        return DOUBLE;
      default:
        return BINARY;
    }
  }
}
