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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.shared.data.arrow;

import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class Schemas {

  public static ArrowType toArrowType(DataType dataType) {
    return toMinorType(dataType).getType();
  }

  public static boolean isNumeric(Types.MinorType minorType) {
    switch (minorType) {
      case INT:
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
        return true;
      default:
        return false;
    }
  }

  public static boolean isNumeric(ArrowType arrowType) {
    return isNumeric(Types.getMinorTypeForArrowType(arrowType));
  }

  public static Types.MinorType getNumericResultType(Types.MinorType... types) {
    return null;
  }

  public static DataType toDataType(ArrowType arrowType) {
    switch (arrowType.getTypeID()) {
      case Bool:
        return DataType.BOOLEAN;
      case Int:
        switch (((ArrowType.Int) arrowType).getBitWidth()) {
          case 32:
            return DataType.INTEGER;
          case 64:
            return DataType.LONG;
          default:
            throw new UnsupportedOperationException("Unsupported arrow type: " + arrowType);
        }
      case FloatingPoint:
        switch (((ArrowType.FloatingPoint) arrowType).getPrecision()) {
          case SINGLE:
            return DataType.FLOAT;
          case DOUBLE:
            return DataType.DOUBLE;
          default:
            throw new UnsupportedOperationException("Unsupported arrow type: " + arrowType);
        }
      case Binary:
        return DataType.BINARY;
      default:
        throw new UnsupportedOperationException("Unsupported arrow type: " + arrowType);
    }
  }

  public static Types.MinorType toMinorType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Types.MinorType.BIT;
      case INTEGER:
        return Types.MinorType.INT;
      case LONG:
        return Types.MinorType.BIGINT;
      case FLOAT:
        return Types.MinorType.FLOAT4;
      case DOUBLE:
        return Types.MinorType.FLOAT8;
      case BINARY:
        return Types.MinorType.VARBINARY;
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  public static Field field(String name, Types.MinorType type) {
    return new Field(name, FieldType.nullable(type.getType()), null);
  }

  public static Field defaultField(Types.MinorType type) {
    return new Field((String) null, FieldType.nullable(type.getType()), null);
  }

  public static Field fieldWithName(Field field, String name) {
    return new Field(name, field.getFieldType(), field.getChildren());
  }

  public static List<Integer> matchPattern(BatchSchema inputSchema, String pattern) {
    return matchPattern(inputSchema.raw(), pattern);
  }

  public static List<Integer> matchPattern(Schema inputSchema, String pattern) {
    List<Integer> indexes = new ArrayList<>();
    Predicate<String> matcher = StringUtils.toColumnMatcher(pattern);
    for (int i = 0; i < inputSchema.getFields().size(); i++) {
      if (matcher.test(inputSchema.getFields().get(i).getName())) {
        indexes.add(i);
      }
    }
    return indexes;
  }
}
