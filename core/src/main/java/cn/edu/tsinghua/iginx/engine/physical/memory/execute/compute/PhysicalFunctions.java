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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute;

import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

public class PhysicalFunctions {

  private PhysicalFunctions() {}

  public static VectorSchemaRoot unnest(
      @WillNotClose BufferAllocator allocator, @WillNotClose VectorSchemaRoot vectorSchemaRoot) {
    List<FieldVector> results = new ArrayList<>();
    for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
      if (fieldVector.getMinorType() == Types.MinorType.STRUCT) {
        try (NonNullableStructVector structVector = (NonNullableStructVector) fieldVector) {
          for (FieldVector childFieldVector : structVector.getChildrenFromFields()) {
            results.add(ValueVectors.slice(allocator, childFieldVector));
          }
        }
      } else {
        results.add(ValueVectors.slice(allocator, fieldVector));
      }
    }
    return new VectorSchemaRoot(results);
  }

  public static IntVector filter(BufferAllocator allocator, BitVector bitmap) {
    int rowCount = bitmap.getValueCount();
    IntVector selectionVector =
        new IntVector("selection", FieldType.notNullable(Types.MinorType.INT.getType()), allocator);
    selectionVector.allocateNew(rowCount);
    int selectionCount = 0;
    for (int i = 0; i < rowCount; i++) {
      if (bitmap.isNull(i) || bitmap.get(i) == 1) {
        selectionVector.set(selectionCount, i);
        selectionCount++;
      }
    }
    selectionVector.setValueCount(selectionCount);
    return selectionVector;
  }

  public static FieldVector take(
      BufferAllocator allocator, IntVector selectionVector, FieldVector input) {
    if (selectionVector.getField().isNullable()) {
      throw new IllegalArgumentException("Selection vector must be not nullable");
    }
    TransferPair transferPair = input.getTransferPair(allocator);
    FieldVector result = (FieldVector) transferPair.getTo();
    result.setInitialCapacity(selectionVector.getValueCount());
    if (result instanceof BaseFixedWidthVector) {
      result.setValueCount(selectionVector.getValueCount());
      for (int i = 0; i < selectionVector.getValueCount(); i++) {
        result.copyFrom(selectionVector.get(i), i, input);
      }
    } else {
      for (int i = 0; i < selectionVector.getValueCount(); i++) {
        result.copyFromSafe(selectionVector.get(i), i, input);
      }
      result.setValueCount(selectionVector.getValueCount());
    }
    return result;
  }

  public static VectorSchemaRoot take(
      BufferAllocator allocator, IntVector selectionVector, VectorSchemaRoot input) {
    List<FieldVector> results = new ArrayList<>();
    for (FieldVector field : input.getFieldVectors()) {
      results.add(take(allocator, selectionVector, field));
    }
    return new VectorSchemaRoot(results);
  }
}
