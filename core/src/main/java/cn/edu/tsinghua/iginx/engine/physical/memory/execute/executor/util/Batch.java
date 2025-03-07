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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util;

import static org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArrowDictionaries;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class Batch implements AutoCloseable {

  private long sequenceNumber; // start from 0
  private final VectorSchemaRoot group;
  private final MapDictionaryProvider dictionaryProvider;
  private final BaseIntVector selection;

  protected Batch(
      @WillCloseWhenClosed VectorSchemaRoot group,
      @WillCloseWhenClosed MapDictionaryProvider dictionaryProvider,
      @Nullable @WillCloseWhenClosed BaseIntVector selection) {
    this.group = Objects.requireNonNull(group);
    this.dictionaryProvider = Objects.requireNonNull(dictionaryProvider);
    this.selection = selection;
  }

  public static Batch of(@WillCloseWhenClosed VectorSchemaRoot compute) {
    return new Batch(compute, new MapDictionaryProvider(), null);
  }

  public static Batch of(
      @WillCloseWhenClosed VectorSchemaRoot compute,
      @WillCloseWhenClosed MapDictionaryProvider dictionaryProvider) {
    return new Batch(compute, dictionaryProvider, null);
  }

  public static Batch of(
      @WillCloseWhenClosed VectorSchemaRoot compute,
      @WillCloseWhenClosed MapDictionaryProvider dictionaryProvider,
      @Nullable @WillCloseWhenClosed BaseIntVector selection) {
    return new Batch(compute, dictionaryProvider, selection);
  }

  public static Batch empty(BufferAllocator allocator, Schema outputSchema) {
    return new Batch(
        VectorSchemaRoot.create(outputSchema, allocator), new MapDictionaryProvider(), null);
  }

  public boolean isEmpty() {
    return getRowCount() == 0;
  }

  public VectorSchemaRoot getData() {
    return group;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  public void setSequenceNumber(long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  public MapDictionaryProvider getDictionaryProvider() {
    return dictionaryProvider;
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
    return new Batch(
        VectorSchemaRoots.slice(allocator, group),
        ArrowDictionaries.slice(allocator, dictionaryProvider),
        getSelectionSlice(allocator));
  }

  public Batch slice(BufferAllocator allocator, int slicedStartIndex, int slicedRowCount) {
    if (selection != null) {
      return new Batch(
          VectorSchemaRoots.slice(allocator, group),
          ArrowDictionaries.slice(allocator, dictionaryProvider),
          ValueVectors.slice(allocator, selection, slicedStartIndex, slicedRowCount));
    }
    return new Batch(
        VectorSchemaRoots.slice(allocator, group, slicedStartIndex, slicedRowCount),
        ArrowDictionaries.slice(allocator, dictionaryProvider),
        null);
  }

  public Batch sliceWith(
      BufferAllocator allocator,
      @WillClose VectorSchemaRoot unnested,
      @WillClose @Nullable BaseIntVector selection) {
    return new Batch(
        unnested,
        ArrowDictionaries.slice(allocator, dictionaryProvider, unnested.getSchema()),
        selection);
  }

  public Batch sliceWith(BufferAllocator allocator, @WillClose @Nullable BaseIntVector selection) {
    return new Batch(
        VectorSchemaRoots.slice(allocator, group),
        ArrowDictionaries.slice(allocator, dictionaryProvider),
        selection);
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
}
