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
    switch (type){
      case Types.NUMERIC:
        if(scale==0){
          if(precision<=1) {
            return BOOLEAN;
          }else if(precision <= 10) {
            return INTEGER;
          }else if(precision <= 19) {
            return LONG;
          }
        }
        return DOUBLE;
      case Types.LONGVARCHAR:
        return BINARY;
      default:
        switch (dataType.toUpperCase()){
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
        return "LONG";
      default:
        throw new IllegalArgumentException("dataType " + dataType + " is not supported");
    }
  }
}
