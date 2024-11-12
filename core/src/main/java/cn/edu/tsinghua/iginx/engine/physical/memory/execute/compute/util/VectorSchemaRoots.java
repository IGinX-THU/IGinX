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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class VectorSchemaRoots {
  private VectorSchemaRoots() {
  }

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
    // check if the row count is the same
    int rowCount = -1;
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      if (rowCount == -1) {
        rowCount = vectorSchemaRoot.getRowCount();
      } else if (rowCount != vectorSchemaRoot.getRowCount()) {
        throw new IllegalArgumentException("Row count is not the same");
      }
    }
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
        fieldVectors.add(ValueVectors.slice(allocator, fieldVector));
      }
    }
    return new VectorSchemaRoot(fieldVectors);
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
    return new VectorSchemaRoot(fieldVectors);
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

  public static VectorSchemaRoot concat(BufferAllocator allocator, Schema schema, Iterable<VectorSchemaRoot> batches) {
    VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
    for (VectorSchemaRoot batch : batches) {
      append(result, batch);
    }
    return result;
  }

  public static VectorSchemaRoot flatten(BufferAllocator allocator, DictionaryProvider dictionaryProvider, VectorSchemaRoot batch, @Nullable BaseIntVector selection) {
    List<FieldVector> resultFieldVectors = new ArrayList<>();
    for (FieldVector fieldVector : batch.getFieldVectors()) {
      resultFieldVectors.add(ValueVectors.flatten(allocator, dictionaryProvider, fieldVector, selection));
    }
    return new VectorSchemaRoot(resultFieldVectors);
  }

  public static VectorSchemaRoot create(BufferAllocator allocator, Schema probeSideSchema, int count) {
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(probeSideSchema, allocator);
    for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
      fieldVector.setInitialCapacity(count);
    }
    vectorSchemaRoot.setRowCount(count);
    return vectorSchemaRoot;
  }
}
