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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import java.util.List;
import java.util.Objects;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

public class Batch implements AutoCloseable {

  private long sequenceNumber; // start from 0
  private final VectorSchemaRoot group;
  private final MapDictionaryProvider dictionaryProvider;

  protected Batch(
      @WillCloseWhenClosed VectorSchemaRoot group,
      @WillCloseWhenClosed MapDictionaryProvider dictionaryProvider) {
    this.group = Objects.requireNonNull(group);
    this.dictionaryProvider = Objects.requireNonNull(dictionaryProvider);
  }

  public static Batch of(@WillCloseWhenClosed VectorSchemaRoot compute) {
    return new Batch(compute, new MapDictionaryProvider());
  }

  public static Batch of(
      @WillCloseWhenClosed VectorSchemaRoot compute,
      @WillCloseWhenClosed MapDictionaryProvider dictionaryProvider) {
    return new Batch(compute, dictionaryProvider);
  }

  public static Batch empty(BufferAllocator allocator, Schema schema) {
    return of(VectorSchemaRoot.create(schema, allocator));
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

  public DictionaryProvider getDictionaryProvider() {
    return dictionaryProvider;
  }

  public VectorSchemaRoot flattened(BufferAllocator allocator) {
    return ArrowDictionaries.flatten(allocator, dictionaryProvider, group, null);
  }

  public Batch slice(BufferAllocator allocator) {
    return new Batch(
        VectorSchemaRoots.slice(allocator, group),
        ArrowDictionaries.slice(allocator, dictionaryProvider));
  }

  public Batch slice(BufferAllocator allocator, int slicedStartIndex, int slicedRowCount) {
    return new Batch(
        VectorSchemaRoots.slice(allocator, group, slicedStartIndex, slicedRowCount),
        ArrowDictionaries.slice(allocator, dictionaryProvider));
  }

  public Batch sliceWith(BufferAllocator allocator, @WillClose VectorSchemaRoot unnested) {
    return new Batch(
        unnested, ArrowDictionaries.slice(allocator, dictionaryProvider, unnested.getSchema()));
  }

  public List<FieldVector> getVectors() {
    return group.getFieldVectors();
  }

  public int getRowCount() {
    return group.getRowCount();
  }

  public FieldVector getVector(int index) {
    return group.getFieldVectors().get(index);
  }

  public Schema getSchema() {
    return ArrowDictionaries.flatten(dictionaryProvider, group.getSchema());
  }

  @Override
  public void close() {
    group.close();
    dictionaryProvider.close();
  }
}
