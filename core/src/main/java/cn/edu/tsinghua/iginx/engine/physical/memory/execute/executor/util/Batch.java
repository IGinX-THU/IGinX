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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.*;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.WillNotClose;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Batch implements AutoCloseable {

  private final VectorSchemaRoot group;
  private final CloseableDictionaryProvider dictionaryProvider;
  private final BaseIntVector selection;

  protected Batch(
      @WillCloseWhenClosed VectorSchemaRoot group,
      @WillCloseWhenClosed CloseableDictionaryProvider dictionaryProvider,
      @Nullable @WillCloseWhenClosed BaseIntVector selection) {
    this.group = Objects.requireNonNull(group);
    this.dictionaryProvider = Objects.requireNonNull(dictionaryProvider);
    this.selection = selection;
  }

  public static Batch of(VectorSchemaRoot compute) {
    return new Batch(compute, DictionaryProviders.emptyClosable(), null);
  }

  public static Batch of(VectorSchemaRoot compute, CloseableDictionaryProvider dictionaryProvider) {
    return new Batch(compute, dictionaryProvider, null);
  }

  public static Batch of(VectorSchemaRoot compute, CloseableDictionaryProvider dictionaryProvider, BaseIntVector selection) {
    return new Batch(compute, dictionaryProvider, null);
  }

  public static Batch empty(BufferAllocator allocator, Schema outputSchema) {
    return new Batch(VectorSchemaRoot.create(outputSchema, allocator), DictionaryProviders.emptyClosable(), null);
  }

  public VectorSchemaRoot getData() {
    return group;
  }

  public CloseableDictionaryProvider getDictionaryProvider() {
    return dictionaryProvider;
  }


  public Batch sliceWith(BufferAllocator allocator, @WillCloseWhenClosed @Nullable BaseIntVector selection) {
    return new Batch(VectorSchemaRoots.slice(allocator, group), dictionaryProvider.slice(allocator), selection);
  }

  @Nullable
  public BaseIntVector getSelection() {
    return selection;
  }

  @Nullable
  public BaseIntVector getSelectionSlice(BufferAllocator allocator) {
    if (selection == null) {
      return null;
    }
    return ValueVectors.slice(allocator, selection);
  }

  public VectorSchemaRoot flattened(BufferAllocator allocator) {
    return VectorSchemaRoots.flatten(allocator, dictionaryProvider, group, selection);
  }

  public Batch slice(BufferAllocator allocator) {
    return new Batch(VectorSchemaRoots.slice(allocator, group), dictionaryProvider.slice(allocator), getSelectionSlice(allocator));
  }

  public Batch slice(BufferAllocator allocator, int slicedStartIndex, int slicedRowCount) {
    if (selection != null) {
      return new Batch(VectorSchemaRoots.slice(allocator, group), dictionaryProvider.slice(allocator), ValueVectors.slice(allocator, selection, slicedStartIndex, slicedRowCount));
    }
    return new Batch(VectorSchemaRoots.slice(allocator, group, slicedStartIndex, slicedRowCount), dictionaryProvider.slice(allocator), null);
  }

  public List<FieldVector> getVectors() {
    return group.getFieldVectors();
  }

  public FieldVector getVector(int index) {
    return group.getFieldVectors().get(index);
  }

  public Schema getSchema() {
    // TODO: flatten schema
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {
    group.close();
    dictionaryProvider.close();
    if (selection != null) {
      selection.close();
    }
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
    if (selection != null) {
      return selection.getValueCount();
    }
    return group.getRowCount();
  }

  public static class Builder implements AutoCloseable {
    private final VectorSchemaRoot root;
    private final ColumnBuilder[] fieldBuilders;

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
          root.getFieldVectors().stream().map(ColumnBuilder::create).toArray(ColumnBuilder[]::new);
    }

    @Override
    public void close() {
      root.close();

    }

    public VectorSchemaRoot raw() {
      return root;
    }

    public Builder append(long key, Object... values) {
      if (values.length + 1 != fieldBuilders.length) {
        throw new IllegalArgumentException(
            "Expected key and "
                + fieldBuilders.length
                + " values, but got "
                + values.length
                + " values");
      }
      fieldBuilders[0].append(key);
      for (int i = 0; i < values.length; i++) {
        fieldBuilders[i + 1].append(values[i]);
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
        return new Batch(table.toVectorSchemaRoot(), DictionaryProviders.emptyClosable(), null);
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
