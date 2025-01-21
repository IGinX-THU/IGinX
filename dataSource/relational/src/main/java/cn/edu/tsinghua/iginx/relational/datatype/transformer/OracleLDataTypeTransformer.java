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

import cn.edu.tsinghua.iginx.relational.query.entity.RelationQueryRowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleLDataTypeTransformer implements IDataTypeTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationQueryRowStream.class);
  private static final OracleLDataTypeTransformer INSTANCE = new OracleLDataTypeTransformer();

  public static OracleLDataTypeTransformer getInstance() {
    return INSTANCE;
  }

  @Override
  public DataType fromEngineType(String dataType, String... parameters) {
    if (parameters == null || parameters.length == 0) {
      return DataType.valueOf(dataType);
    }
    if (dataType.equalsIgnoreCase("VARCHAR2")) {
      return BINARY;
    } else if (dataType.equalsIgnoreCase("NUMBER")) {
      int columnSize = Integer.parseInt(parameters[0]);
      if (columnSize == 1) {
        return BOOLEAN;
      } else if (columnSize >= 1 && columnSize <= 10) {
        return INTEGER;
      } else if (columnSize == 126) { // from ResultSet api
        return FLOAT;
      } else if (columnSize == 38) { // from ResultSet api
        return DOUBLE;
      } else {
        return LONG;
      }
    } else if (dataType.equalsIgnoreCase("FLOAT")) { // from getColumns api
      return FLOAT;
    } else {
      LOGGER.error("column type {} is not supported", dataType);
    }
    return null;
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
        return "FLOAT";
      case DOUBLE:
        return "NUMBER(38, 10)"; // "NUMBER(38, 10)";FLOAT
      case BINARY:
      default:
        return "VARCHAR2(4000)";
    }
  }
}
