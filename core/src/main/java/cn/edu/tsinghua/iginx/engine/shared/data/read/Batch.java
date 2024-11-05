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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

public class Batch implements AutoCloseable {

  private final transient BatchSchema schema;
  private final VectorSchemaRoot group;

  protected Batch(@WillCloseWhenClosed VectorSchemaRoot group) {
    this.group = Objects.requireNonNull(group);
    BatchSchema.Builder builder = new BatchSchema.Builder();
    for (Field field : group.getSchema().getFields()) {
      builder.addField(field);
    }
    this.schema = builder.build();
  }

  public static Batch of(VectorSchemaRoot compute) {
    return new Batch(compute);
  }

  public VectorSchemaRoot raw() {
    return group;
  }

  public List<FieldVector> getVectors() {
    return group.getFieldVectors();
  }

  public FieldVector getVector(int index) {
    return group.getFieldVectors().get(index);
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public FieldVector getKeyVector() {
    FieldVector key = group.getVector(BatchSchema.KEY);
    if (key == null) {
      throw new IllegalStateException("Key vector is missing");
    }
    return key;
  }

  @Override
  public void close() {
    group.close();
  }

  public boolean isEmpty() {
    return group.getRowCount() == 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Batch that = (Batch) o;
    return Objects.equals(group, that.group);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(group);
  }

  @Override
  public String toString() {
    return group.toString();
  }

  public int getRowCount() {
    return group.getRowCount();
  }

  public static class Builder implements AutoCloseable {
    private final VectorSchemaRoot root;
    private final List<ColumnBuilder> fieldBuilders;

    public Builder(@WillNotClose BufferAllocator allocator, BatchSchema schema, int rowCount) {
      this(allocator, schema);
      for (FieldVector vector : root.getFieldVectors()) {
        vector.setInitialCapacity(rowCount);
        if (vector instanceof BaseFixedWidthVector) {
          ((BaseFixedWidthVector) vector).allocateNew(rowCount);
        }
      }
    }

    public Builder(@WillNotClose BufferAllocator allocator, BatchSchema schema) {
      this.root = VectorSchemaRoot.create(schema.raw(), allocator);
      this.fieldBuilders =
          root.getFieldVectors().stream().map(ColumnBuilder::create).collect(Collectors.toList());
    }

    @Override
    public void close() {
      root.close();
    }

    public VectorSchemaRoot raw() {
      return root;
    }

    public Builder append(long key, Object... values) {
      if (values.length + 1 != fieldBuilders.size()) {
        throw new IllegalArgumentException(
            "Expected key and "
                + fieldBuilders.size()
                + " values, but got "
                + values.length
                + " values");
      }
      fieldBuilders.get(0).append(key);
      for (int i = 0; i < values.length; i++) {
        fieldBuilders.get(i + 1).append(values[i]);
      }
      return this;
    }

    public Builder append(Object... values) {
      if (values.length != fieldBuilders.size()) {
        throw new IllegalArgumentException(
            "Expected " + fieldBuilders.size() + " values, but got " + values.length);
      }
      for (int i = 0; i < values.length; i++) {
        fieldBuilders.get(i).append(values[i]);
      }
      return this;
    }

    public Builder append(ValueHolder... values) {
      if (values.length != fieldBuilders.size()) {
        throw new IllegalArgumentException(
            "Expected " + fieldBuilders.size() + " values, but got " + values.length);
      }
      for (int i = 0; i < values.length; i++) {
        fieldBuilders.get(i).append(values[i]);
      }
      return this;
    }

    public Batch build(int rowCount) {
      root.setRowCount(rowCount);
      try (Table table = new Table(root)) {
        return new Batch(table.toVectorSchemaRoot());
      }
    }

    public ColumnBuilder getField(int index) {
      return fieldBuilders.get(index);
    }

    public TransferPair getTransferPair(int targetIndex, ValueVector source) {
      return source.makeTransferPair(root.getFieldVectors().get(targetIndex));
    }
  }
}
