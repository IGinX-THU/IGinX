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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.arrow;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.function.IntPredicate;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.StableVectorComparator;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.VectorBatchAppender;

public class ArrowVectors {

  public static ValueVector of(
      boolean nullable, ColumnKey columnKey, DataType type, BufferAllocator allocator) {
    Preconditions.checkNotNull(columnKey, "columnKey");
    Preconditions.checkNotNull(type, "type");
    Preconditions.checkNotNull(allocator, "allocator");

    Field field = ArrowFields.of(nullable, columnKey, type);

    switch (type) {
      case BOOLEAN:
        return new BitVector(field, allocator);
      case INTEGER:
        return new IntVector(field, allocator);
      case LONG:
        return new BigIntVector(field, allocator);
      case FLOAT:
        return new Float4Vector(field, allocator);
      case DOUBLE:
        return new Float8Vector(field, allocator);
      case BINARY:
        return new VarBinaryVector(field, allocator);
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  public static ValueVector nullable(
      ColumnKey columnKey, DataType type, BufferAllocator allocator) {
    return of(true, columnKey, type, allocator);
  }

  public static ValueVector nonnull(ColumnKey columnKey, DataType type, BufferAllocator allocator) {
    return of(false, columnKey, type, allocator);
  }

  public static BigIntVector key(BufferAllocator allocator) {
    return (BigIntVector) nonnull(ColumnKey.KEY, DataType.LONG, allocator);
  }

  public static <V extends ValueVector> V slice(V vector) {
    return slice(vector, 0, vector.getValueCount());
  }

  public static <V extends ValueVector> V slice(V vector, BufferAllocator allocator) {
    return slice(vector, 0, vector.getValueCount(), allocator);
  }

  public static <V extends ValueVector> V slice(V vector, int start, int valueCount) {
    return slice(vector, start, valueCount, vector.getAllocator());
  }

  @SuppressWarnings("unchecked")
  public static <V extends ValueVector> V slice(
      V vector, int start, int length, BufferAllocator allocator) {
    if (length == 0) {
      // splitAndTransfer 不能处理 length == 0 的情况
      return like(vector, allocator);
    }
    TransferPair transferPair = vector.getTransferPair(allocator);
    transferPair.splitAndTransfer(start, length);
    // TODO: splitAndTransferValidityBuffer() 不能正确地对 ValidityBuffer 进行跨 allocator 的 transfer
    //       所以这里只好先 splitAndTransfer 然后再 transfer，个人猜测这是一个需要汇报给 Arrow 的 bug
    try (V slice = (V) transferPair.getTo()) {
      return transfer(slice, allocator);
    }
  }

  @SuppressWarnings("unchecked")
  public static <V extends ValueVector> V transfer(V vector, BufferAllocator allocator) {
    TransferPair transferPair = vector.getTransferPair(allocator);
    transferPair.transfer();
    return (V) transferPair.getTo();
  }

  @SuppressWarnings("unchecked")
  public static <V extends ValueVector> V like(V vector, BufferAllocator allocator) {
    return (V) vector.getTransferPair(allocator).getTo();
  }

  public static boolean isStrictlyOrdered(ValueVector vector) {
    VectorValueComparator<ValueVector> comparator =
        DefaultVectorComparators.createDefaultComparator(vector);
    comparator.attachVector(vector);
    for (int i = 1; i < vector.getValueCount(); i++) {
      if (comparator.compare(i - 1, i) >= 0) {
        return false;
      }
    }
    return true;
  }

  public static IntVector range(int start, int end, BufferAllocator allocator) {
    IntVector indexes = nonnullIntVector("", allocator);
    indexes.allocateNew(end - start);
    indexes.setValueCount(end - start);
    for (int i = start; i < end; i++) {
      indexes.set(i - start, i);
    }
    return indexes;
  }

  public static IntVector stableSortIndexes(ValueVector vector, BufferAllocator allocator) {
    VectorValueComparator<ValueVector> defaultComparator =
        DefaultVectorComparators.createDefaultComparator(vector);
    VectorValueComparator<ValueVector> stableComparator =
        new StableVectorComparator<>(defaultComparator);

    IntVector indexes = nonnullIntVector("", allocator);
    indexes.setValueCount(vector.getValueCount());

    new IndexSorter<>().sort(vector, indexes, stableComparator);

    return indexes;
  }

  public static IntVector nonnullIntVector(String name, BufferAllocator allocator) {
    return new IntVector(name, FieldType.notNullable(Types.MinorType.INT.getType()), allocator);
  }

  public static void dedupSortedIndexes(ValueVector vector, IntVector indexes) {
    VectorValueComparator<ValueVector> defaultComparator =
        DefaultVectorComparators.createDefaultComparator(vector);
    defaultComparator.attachVector(vector);

    int count = indexes.getValueCount();
    int unique = 0;
    for (int i = 1; i < count; i++) {
      if (defaultComparator.compare(indexes.get(i - 1), indexes.get(i)) != 0) {
        if (unique != i - 1) {
          indexes.set(unique, indexes.get(i - 1));
        }
        unique++;
      }
    }
    if (count > 0) {
      indexes.set(unique, indexes.get(count - 1));
      unique++;
    }
    indexes.setValueCount(unique);
  }

  public static void append(ValueVector to, ValueVector from) {
    if (to.getValueCount() == 0) {
      to.allocateNew();
    }
    VectorBatchAppender.batchAppend(to, from);
  }

  public static void collect(IntVector indexes, Iterable<Integer> values) {
    int offset = indexes.getValueCount();
    for (Integer value : values) {
      if (value == null) indexes.setNull(offset);
      else indexes.setSafe(offset, value);
      offset++;
    }
    indexes.setValueCount(offset);
  }

  public static void filter(IntVector indexes, IntPredicate predicate) {
    int count = indexes.getValueCount();
    int offset = 0;
    for (int i = 0; i < count; i++) {
      if (predicate.test(indexes.get(i))) {
        indexes.set(offset, indexes.get(i));
        offset++;
      }
    }
    indexes.setValueCount(offset);
  }
}
