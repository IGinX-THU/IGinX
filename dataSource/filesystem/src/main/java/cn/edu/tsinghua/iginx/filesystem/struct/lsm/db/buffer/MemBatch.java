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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.VectorBatchAppender;

@ThreadSafe
public final class MemBatch implements NoexceptAutoCloseable {

  private final BigIntVector keyVector;
  private ImmutableList<FieldVector> fieldVectors;

  public MemBatch(List<Field> fields, int initialCapacity, BufferAllocator allocator) {
    this(
        new BigIntVector("", allocator),
        fields.stream()
            .map(f -> f.createVector(allocator))
            .collect(ImmutableList.toImmutableList()));
    this.keyVector.setInitialCapacity(initialCapacity);
    for (FieldVector valueVector : fieldVectors) {
      valueVector.setInitialCapacity(initialCapacity);
    }
  }

  private MemBatch(
      @WillCloseWhenClosed BigIntVector keyVector,
      @WillCloseWhenClosed ImmutableList<FieldVector> fieldVectors) {
    this.keyVector = keyVector;
    this.fieldVectors = fieldVectors;
  }

  public synchronized MemBatch split(IntLinkedOpenHashSet fields, BufferAllocator allocator) {
    BigIntVector keyVectorReplicate = copy(keyVector, allocator);
    ImmutableList<FieldVector> splitVector =
        fields
            .intStream()
            .mapToObj(fieldVectors::get)
            .map(
                v -> {
                  FieldVector sv = transfer(v, allocator);
                  v.close();
                  return sv;
                })
            .collect(ImmutableList.toImmutableList());
    this.fieldVectors =
        IntStream.range(0, fieldVectors.size())
            .filter(i -> !fields.contains(i))
            .mapToObj(fieldVectors::get)
            .map(v -> transfer(v, v.getAllocator()))
            .collect(ImmutableList.toImmutableList());
    return new MemBatch(keyVectorReplicate, splitVector);
  }

  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    return new Snapshot(
        keyVector,
        fieldVectors,
        IntStream.range(0, fieldVectors.size()).toArray(),
        0,
        keyVector.getValueCount(),
        allocator);
  }

  @SuppressWarnings("unchecked")
  private static <V extends ValueVector> V transfer(V vector, BufferAllocator allocator) {
    TransferPair transferPair = vector.getTransferPair(allocator);
    transferPair.transfer();
    return (V) transferPair.getTo();
  }

  @SuppressWarnings("unchecked")
  private static <V extends ValueVector> V copy(V vector, BufferAllocator allocator) {
    TransferPair transferPair = vector.getTransferPair(allocator);
    V replicate = (V) transferPair.getTo();
    replicate.setInitialCapacity(vector.getValueCapacity());
    append(replicate, vector);
    return replicate;
  }

  public synchronized void append(Snapshot batch) {
    append(keyVector, batch.keyVector);

    List<FieldVector> targets = this.fieldVectors;
    List<FieldVector> sources = batch.getFieldVectors();
    Preconditions.checkArgument(targets.size() == sources.size());
    for (int i = 0; i < sources.size(); i++) {
      FieldVector target = targets.get(i);
      FieldVector source = sources.get(i);
      Preconditions.checkArgument(target.getField().equals(source.getField()));
      append(target, source);
    }
  }

  private static void append(ValueVector target, ValueVector source) {
    if (target.getValueCapacity() == 0) {
      target.allocateNew();
    }
    VectorBatchAppender.batchAppend(target, source);
  }

  public synchronized int getValueCount() {
    return keyVector.getValueCount();
  }

  @Override
  public synchronized void close() {
    keyVector.close();
    fieldVectors.forEach(FieldVector::close);
  }

  @Immutable
  public static final class Snapshot implements NoexceptAutoCloseable {

    private final BigIntVector keyVector;
    private final ImmutableList<FieldVector> fieldVectors;

    public Snapshot(
        @WillCloseWhenClosed BigIntVector keyVector,
        @WillCloseWhenClosed ImmutableList<FieldVector> fieldVectors) {
      for (FieldVector fieldVector : fieldVectors) {
        Preconditions.checkArgument(keyVector.getValueCount() == fieldVector.getValueCount());
      }
      this.keyVector = keyVector;
      this.fieldVectors = fieldVectors;
    }

    private Snapshot(
        BigIntVector keyVector,
        List<FieldVector> fieldVectors,
        int[] fields,
        int startIndex,
        int length,
        BufferAllocator allocator) {
      this(
          slice(keyVector, startIndex, length, allocator),
          Arrays.stream(fields)
              .mapToObj(fieldVectors::get)
              .map(v -> slice(v, startIndex, length, allocator))
              .collect(ImmutableList.toImmutableList()));
    }

    @SuppressWarnings("unchecked")
    private static <V extends ValueVector> V slice(
        V vector, int startIndex, int length, BufferAllocator allocator) {
      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.splitAndTransfer(startIndex, length);
      return (V) transferPair.getTo();
    }

    public int getValueCount() {
      return keyVector.getValueCount();
    }

    public BigIntVector getKeyVector() {
      return keyVector;
    }

    public List<FieldVector> getFieldVectors() {
      return fieldVectors;
    }

    public Snapshot slice(int[] fields, BufferAllocator allocator) {
      return slice(fields, 0, getValueCount(), allocator);
    }

    public Snapshot slice(int startIndex, int length, BufferAllocator allocator) {
      return slice(
          IntStream.range(0, getFieldVectors().size()).toArray(), startIndex, length, allocator);
    }

    public Snapshot slice(int[] fields, int startIndex, int length, BufferAllocator allocator) {
      return new Snapshot(keyVector, fieldVectors, fields, startIndex, length, allocator);
    }

    @Override
    public void close() {
      keyVector.close();
      fieldVectors.forEach(FieldVector::close);
    }
  }
}
