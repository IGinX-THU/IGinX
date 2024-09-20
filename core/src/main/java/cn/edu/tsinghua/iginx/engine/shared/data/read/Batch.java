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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Table;

public class Batch implements AutoCloseable {

  private final Table table;
  private final transient BatchSchema schema;

  protected Batch(@WillCloseWhenClosed Table table) {
    this.table = table;
    this.schema = new BatchSchema(table.getSchema());
  }

  public Batch(@WillCloseWhenClosed Table table, BatchSchema schema) {
    this.table = table;
    this.schema = schema;
  }

  @Override
  public void close() {
    table.close();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Batch that = (Batch) o;
    return Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(table);
  }

  @Override
  public String toString() {
    return table.toString();
  }

  public Table raw() {
    return table;
  }

  public long getRowCount() {
    return table.getRowCount();
  }

  public static Builder builder(
      BufferAllocator allocator, BatchSchema schema, int initialCapacity) {
    return new Builder(allocator, schema, initialCapacity);
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public static class Builder implements AutoCloseable {
    private final VectorSchemaRoot root;
    private final List<ColumnBuilder> fieldBuilders = new ArrayList<>();

    Builder(@WillNotClose BufferAllocator allocator, BatchSchema schema, int initialCapacity) {
      this.root = VectorSchemaRoot.create(schema.raw(), allocator);
      for (FieldVector vector : root.getFieldVectors()) {
        vector.setInitialCapacity(initialCapacity);
        fieldBuilders.add(ColumnBuilder.create(vector));
      }
    }

    @Override
    public void close() {
      root.close();
    }

    public Builder appendRow(long key, Object... values) {
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

    public Builder appendRow(Object... values) {
      if (values.length != fieldBuilders.size()) {
        throw new IllegalArgumentException(
            "Expected " + fieldBuilders.size() + " values, but got " + values.length);
      }
      for (int i = 0; i < values.length; i++) {
        fieldBuilders.get(i).append(values[i]);
      }
      return this;
    }

    public Builder setRowCount(int rowCount) {
      root.setRowCount(rowCount);
      return this;
    }

    public Batch build() {
      Integer rowCount = null;
      for (FieldVector fieldBuilder : root.getFieldVectors()) {
        if (rowCount == null) {
          rowCount = fieldBuilder.getValueCount();
        } else if (rowCount != fieldBuilder.getValueCount()) {
          throw new IllegalStateException("Row counts are inconsistent");
        }
      }
      Batch result = new Batch(new Table(root.getFieldVectors()));
      root.clear();
      return result;
    }

    public ColumnBuilder getField(int index) {
      return fieldBuilders.get(index);
    }
  }
}
