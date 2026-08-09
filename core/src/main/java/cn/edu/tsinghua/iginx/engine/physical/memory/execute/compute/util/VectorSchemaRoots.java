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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

public class VectorSchemaRoots {
  private VectorSchemaRoots() {}

  public static Object[] get(VectorSchemaRoot vectorSchemaRoot, int index) {
    List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
    Object[] objects = new Object[fieldVectors.size()];
    for (int i = 0; i < objects.length; i++) {
      objects[i] = fieldVectors.get(i).getObject(index);
    }
    return objects;
  }

  public static VectorSchemaRoot join(
      BufferAllocator allocator, Iterable<VectorSchemaRoot> vectorSchemaRoots) {
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      fieldVectors.addAll(vectorSchemaRoot.getFieldVectors());
    }
    // check if all the field vectors have the same row count
    int rowCount = -1;
    for (FieldVector fieldVector : fieldVectors) {
      if (rowCount == -1) {
        rowCount = fieldVector.getValueCount();
      } else {
        Preconditions.checkArgument(
            rowCount == fieldVector.getValueCount(),
            "All field vectors must have the same row count");
      }
    }
    List<FieldVector> slicedFieldVectors = new ArrayList<>();
    for (FieldVector fieldVector : fieldVectors) {
      slicedFieldVectors.add(ValueVectors.slice(allocator, fieldVector));
    }
    return VectorSchemaRoots.create(slicedFieldVectors, rowCount == -1 ? 0 : rowCount);
  }

  public static VectorSchemaRoot join(
      BufferAllocator allocator, VectorSchemaRoot... vectorSchemaRoots) {
    return join(allocator, Arrays.asList(vectorSchemaRoots));
  }

  public static VectorSchemaRoot slice(
      BufferAllocator allocator, VectorSchemaRoot batch, int start, int count) {
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (FieldVector fieldVector : batch.getFieldVectors()) {
      fieldVectors.add(ValueVectors.slice(allocator, fieldVector, start, count));
    }
    return VectorSchemaRoots.create(fieldVectors, count);
  }

  public static VectorSchemaRoot slice(BufferAllocator allocator, VectorSchemaRoot batch) {
    return slice(allocator, batch, 0, batch.getRowCount());
  }

  public static void transfer(VectorSchemaRoot target, VectorSchemaRoot source) {
    for (int i = 0; i < target.getFieldVectors().size(); i++) {
      source.getFieldVectors().get(i).makeTransferPair(target.getFieldVectors().get(i)).transfer();
    }
  }

  public static VectorSchemaRoot transfer(BufferAllocator allocator, VectorSchemaRoot source) {
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (FieldVector fieldVector : source.getFieldVectors()) {
      fieldVectors.add(ValueVectors.transfer(allocator, fieldVector));
    }
    return new VectorSchemaRoot(fieldVectors);
  }

  public static void append(VectorSchemaRoot target, VectorSchemaRoot source) {
    Preconditions.checkArgument(Objects.equals(target.getSchema(), source.getSchema()));

    if (target.getRowCount() == 0) {
      for (FieldVector fieldVector : target.getFieldVectors()) {
        fieldVector.allocateNew();
      }
    }

    VectorSchemaRootAppender.append(false, target, source);
  }

  public static VectorSchemaRoot concat(
      BufferAllocator allocator, Schema schema, Iterable<VectorSchemaRoot> batches) {
    VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
    for (VectorSchemaRoot batch : batches) {
      append(result, batch);
    }
    return result;
  }

  public static VectorSchemaRoot create(
      BufferAllocator allocator, Schema probeSideSchema, int count) {
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(probeSideSchema, allocator);
    for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
      fieldVector.setInitialCapacity(count);
    }
    vectorSchemaRoot.setRowCount(count);
    return vectorSchemaRoot;
  }

  public static List<VectorSchemaRoot> partition(
      BufferAllocator allocator, VectorSchemaRoot data, int batchRowCount) {
    Preconditions.checkArgument(batchRowCount > 0, "size must be greater than 0");
    int totalRowCount = data.getRowCount();
    List<VectorSchemaRoot> result = new ArrayList<>();
    for (int startIndex = 0; startIndex < totalRowCount; startIndex += batchRowCount) {
      int sliceSize = Math.min(batchRowCount, totalRowCount - startIndex);
      result.add(slice(allocator, data, startIndex, sliceSize));
    }
    return result;
  }

  public static VectorSchemaRoot create(@WillClose List<FieldVector> vectors, int rowCount) {
    Preconditions.checkArgument(vectors.stream().allMatch(v -> v.getValueCount() == rowCount));
    return new VectorSchemaRoot(
        new Schema(vectors.stream().map(FieldVector::getField).collect(Collectors.toList())),
        vectors,
        rowCount);
  }

  public static VectorSchemaRoot select(
      BufferAllocator allocator, VectorSchemaRoot data, @Nullable BaseIntVector selection) {
    if (selection == null) {
      return slice(allocator, data);
    }
    List<FieldVector> resultVectors = new ArrayList<>();
    for (FieldVector fieldVector : data.getFieldVectors()) {
      resultVectors.add(ValueVectors.select(allocator, fieldVector, selection));
    }
    return create(resultVectors, selection.getValueCount());
  }
}
