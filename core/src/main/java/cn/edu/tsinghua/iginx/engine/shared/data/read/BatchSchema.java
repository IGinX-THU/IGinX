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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class BatchSchema {

  public static final Field KEY =
      Field.notNullable(Constants.KEY, Types.MinorType.BIGINT.getType());

  private final Schema schema;

  protected BatchSchema(Schema schema) {
    this.schema = Objects.requireNonNull(schema);
  }

  public boolean hasKey() {
    return !schema.getFields().isEmpty()
        && schema.getFields().get(0).getName().equals(Constants.KEY);
  }

  public Field getField(int index) {
    return schema.getFields().get(index);
  }

  public String getFieldName(int index) {
    return schema.getFields().get(index).getName();
  }

  public ArrowType getFieldArrowType(int index) {
    return schema.getFields().get(index).getType();
  }

  public Map<String, String> getTag(int index) {
    return schema.getFields().get(index).getMetadata();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BatchSchema that = (BatchSchema) o;
    return Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schema);
  }

  @Override
  public String toString() {
    return schema.toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public Schema raw() {
    return schema;
  }

  public static class Builder {

    protected final List<Field> fields = new ArrayList<>();

    protected Builder() {}

    public Builder withKey() {
      if (!fields.isEmpty()) {
        throw new IllegalStateException("Key field must be the first field");
      }
      fields.add(KEY);
      return this;
    }

    protected Builder addField(Field field) {
      if (Objects.equals(field.getName(), KEY.getName())) {
        throw new IllegalArgumentException("Field name cannot be duplicated with key field");
      }
      fields.add(field);
      return this;
    }

    protected Builder addField(String name, FieldType type) {
      return addField(new Field(name, type, null));
    }

    public Builder addField(String name, ArrowType type, Map<String, String> tags) {
      return addField(name, new FieldType(true, type, null, tags));
    }

    public Builder addField(String name, ArrowType type) {
      return addField(name, type, null);
    }

    public Builder addField(String name, DataType type, Map<String, String> tags) {
      return addField(name, toArrowType(type), tags);
    }

    public Builder addField(String name, DataType type) {
      return addField(name, toArrowType(type));
    }

    public BatchSchema build() {
      return new BatchSchema(new Schema(fields));
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

  public static ArrowType toArrowType(DataType dataType) {
    return toMinorType(dataType).getType();
  }
}
