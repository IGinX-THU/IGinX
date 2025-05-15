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

public class DamengDataTypeTransformer implements IDataTypeTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationQueryRowStream.class);
  private static final DamengDataTypeTransformer INSTANCE = new DamengDataTypeTransformer();

  public static DamengDataTypeTransformer getInstance() {
    return INSTANCE;
  }

  @Override
  public DataType fromEngineType(String dataType, int precision, int scale) {
    if (dataType.equalsIgnoreCase("BYTE") || dataType.equalsIgnoreCase("TINYINT")) {
      return BOOLEAN;
    } else if (dataType.equalsIgnoreCase("BIGINT")) {
      return LONG;
    } else if (dataType.equalsIgnoreCase("INT")
        || dataType.equalsIgnoreCase("SMALLINT")
        || dataType.equalsIgnoreCase("NUMERIC")
        || dataType.equalsIgnoreCase("INTEGER")) {
      return INTEGER;
    } else if (dataType.equalsIgnoreCase("FLOAT")
        || dataType.equalsIgnoreCase("REAL")) { // from getColumns api
      return FLOAT;
    } else if (dataType.equalsIgnoreCase("DOUBLE PRECISION")
        || dataType.equalsIgnoreCase("DOUBLE")
        || dataType.equalsIgnoreCase("DECIMAL")
        || dataType.equalsIgnoreCase("BigDecimal")) {
      return DOUBLE;
    } else {
      return BINARY;
    }
  }

  public String toEngineType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return "TINYINT";
      case INTEGER:
        return "INT";
      case LONG:
        return "BIGINT";
      case FLOAT:
        return "REAL";
      case DOUBLE:
        return "DOUBLE PRECISION";
      case BINARY:
      default:
        return "VARCHAR(4000)";
    }
  }
}
