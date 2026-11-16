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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.*;

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

  public static List<Types.MinorType> getNumericTypes() {
    return Arrays.asList(
        Types.MinorType.INT,
        Types.MinorType.BIGINT,
        Types.MinorType.FLOAT4,
        Types.MinorType.FLOAT8);
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

  public static Field nullableField(String name, Types.MinorType type) {
    return field(name, true, type);
  }

  public static Field field(String name, boolean nullable, Types.MinorType type) {
    return new Field(name, new FieldType(nullable, type.getType(), null), null);
  }

  public static Field defaultField(Types.MinorType type) {
    return new Field((String) null, FieldType.nullable(type.getType()), null);
  }

  public static Field fieldWithName(Field field, String name) {
    return new Field(name, field.getFieldType(), field.getChildren());
  }

  public static Field fieldWithNullable(Field field, boolean nullable) {
    return new Field(
        field.getName(),
        new FieldType(
            nullable,
            field.getFieldType().getType(),
            field.getFieldType().getDictionary(),
            field.getFieldType().getMetadata()),
        field.getChildren());
  }

  public static Field fieldWith(
      Field field,
      String name,
      DictionaryEncoding dictionaryEncoding,
      Map<String, String> metadata) {
    return new Field(
        name,
        new FieldType(field.isNullable(), field.getType(), dictionaryEncoding, metadata),
        field.getChildren());
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

  public static List<Integer> matchPatternIgnoreKey(Schema inputSchema, String pattern) {
    List<Integer> indexes = matchPattern(inputSchema, pattern);
    indexes.removeIf(i -> BatchSchema.KEY.equals(inputSchema.getFields().get(i)));
    return indexes;
  }

  public static List<Integer> matchPatternIgnoreKey(
      Schema inputSchema, Collection<String> patterns) {
    List<Integer> indexes = new ArrayList<>();
    for (String pattern : patterns) {
      indexes.addAll(matchPatternIgnoreKey(inputSchema, pattern));
    }
    Collections.sort(indexes);
    return indexes;
  }

  public static Schema of(Field... fields) {
    return new Schema(Arrays.asList(fields));
  }

  public static Schema of(FieldVector... fieldVectors) {
    return of(Arrays.asList(fieldVectors));
  }

  public static Schema of(List<FieldVector> fieldVectors) {
    return new Schema(
        fieldVectors.stream().map(FieldVector::getField).collect(Collectors.toList()));
  }

  public static Types.MinorType minorTypeOf(Field inputField) {
    return Types.getMinorTypeForArrowType(inputField.getFieldType().getType());
  }

  public static Schema merge(List<Schema> schemas) {
    List<Field> fields = new ArrayList<>();
    for (Schema schema : schemas) {
      fields.addAll(schema.getFields());
    }
    return new Schema(fields);
  }

  public static Schema merge(Schema... schemas) {
    return merge(Arrays.asList(schemas));
  }
}
