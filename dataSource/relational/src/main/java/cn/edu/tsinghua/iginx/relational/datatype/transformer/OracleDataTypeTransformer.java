package cn.edu.tsinghua.iginx.relational.datatype.transformer;

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
    return switch (type) {
      case Types.NUMERIC -> {
        if (scale == 0) {
          if (precision <= 1) {
            yield BOOLEAN;
          } else if (precision <= 10) {
            yield INTEGER;
          } else if (precision <= 19) {
            yield LONG;
          }
        }
        yield DOUBLE;
      }
      case Types.VARCHAR -> BINARY;
      default -> switch (dataType.toUpperCase()) {
        case "BINARY_FLOAT" -> FLOAT;
        case "BINARY_DOUBLE" -> DOUBLE;
        default -> BINARY;
      };
    };
  }

  public String toEngineType(DataType dataType) {
    return switch (dataType) {
      case BOOLEAN -> "NUMBER(1)";
      case INTEGER -> "NUMBER(10)";
      case LONG -> "NUMBER(19)";
      case FLOAT -> "BINARY_FLOAT";
      case DOUBLE -> "BINARY_DOUBLE";
      case BINARY -> "VARCHAR2(4000)";
    };
  }
}
