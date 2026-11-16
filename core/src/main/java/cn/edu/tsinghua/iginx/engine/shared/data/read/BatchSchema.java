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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class BatchSchema {

  public static final Field KEY =
      Field.notNullable(Constants.KEY, Types.MinorType.BIGINT.getType());

  private final Schema schema;
  private final Map<String, Integer> indexMap;

  protected BatchSchema(Schema schema) {
    this.schema = Objects.requireNonNull(schema);
    this.indexMap = new HashMap<>();
    for (int i = 0; i < schema.getFields().size(); i++) {
      indexMap.put(schema.getFields().get(i).getName(), i);
    }
  }

  public static BatchSchema of(Schema schema) {
    BatchSchema.Builder builder = new BatchSchema.Builder();
    for (Field field : schema.getFields()) {
      builder.addField(field);
    }
    return builder.build();
  }

  public static BatchSchema empty() {
    return new BatchSchema(new Schema(Collections.emptyList()));
  }

  public boolean hasKey() {
    return !schema.getFields().isEmpty()
        && schema.getFields().get(0).getName().equals(Constants.KEY);
  }

  public Field getField(int index) {
    return schema.getFields().get(index);
  }

  public Batch emptyBatch(BufferAllocator allocator) {
    try (BatchBuilder builder = new BatchBuilder(allocator, this)) {
      return builder.build(0);
    }
  }

  public int getFieldCount() {
    return schema.getFields().size();
  }

  @Nullable
  public Integer indexOf(String name) {
    return indexMap.get(name);
  }

  public String getName(int index) {
    return schema.getFields().get(index).getName();
  }

  public ArrowType getArrowType(int index) {
    return schema.getFields().get(index).getType();
  }

  public DataType getDataType(int index) {
    return Schemas.toDataType(getArrowType(index));
  }

  public FieldType getFieldType(int index) {
    return schema.getFields().get(index).getFieldType();
  }

  public Types.MinorType getMinorType(int index) {
    return Types.getMinorTypeForArrowType(getArrowType(index));
  }

  public Types.MinorType[] getMinorTypeArray() {
    return schema.getFields().stream()
        .map(Field::getType)
        .map(Types::getMinorTypeForArrowType)
        .toArray(Types.MinorType[]::new);
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

  public int getKeyIndex() {
    if (!hasKey()) {
      return -1;
    }
    return 0;
  }

  public static class Builder {

    protected final List<Field> fields = new ArrayList<>();

    protected Builder() {}

    public Builder addField(Field field) {
      String name = field.getName();
      Objects.requireNonNull(field);
      Objects.requireNonNull(field.getName());
      if (name.equals(Constants.KEY)) {
        if (!fields.isEmpty()) {
          throw new IllegalStateException("Key field must be the first field");
        } else if (!field.equals(KEY)) {
          throw new IllegalStateException(
              "Key field must be defined as " + KEY + " but was " + field);
        }
      }
      fields.add(field);
      return this;
    }

    public Builder addField(String name, FieldType type) {
      return addField(new Field(name, type, null));
    }

    public Builder withKey() {
      addField(KEY);
      return this;
    }

    public Builder addField(String name, ArrowType type, @Nullable Map<String, String> tags) {
      return addField(name, new FieldType(true, type, null, tags));
    }

    public Builder addField(String name, ArrowType type) {
      return addField(name, type, null);
    }

    public Builder addField(String name, DataType type, @Nullable Map<String, String> tags) {
      return addField(name, Schemas.toArrowType(type), tags);
    }

    public Builder addField(String name, DataType type) {
      return addField(name, Schemas.toArrowType(type));
    }

    public BatchSchema build() {
      return new BatchSchema(new Schema(fields));
    }
  }
}
