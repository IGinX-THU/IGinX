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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

public class PhysicalFunctions {

  private PhysicalFunctions() {}

  public static Schema unnest(Schema schema) {
    List<Field> fields = new ArrayList<>();
    for (Field field : schema.getFields()) {
      if (Types.getMinorTypeForArrowType(field.getType()) == Types.MinorType.STRUCT) {
        fields.addAll(field.getChildren());
      } else {
        fields.add(field);
      }
    }
    return new Schema(fields);
  }

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

  public static <OUTPUT extends FieldVector> void takeTo(
      IntVector selection, OUTPUT output, OUTPUT input) {
    if (selection.getField().isNullable()) {
      throw new IllegalArgumentException("Selection vector must be not nullable");
    }

    int outputOffset = output.getValueCount();
    int outputRowCount = outputOffset + selection.getValueCount();
    if (output instanceof BaseFixedWidthVector) {
      output.setValueCount(outputRowCount);
      for (int selectionIndex = 0; selectionIndex < selection.getValueCount(); selectionIndex++) {
        int outputIndex = outputOffset + selectionIndex;
        output.copyFrom(selection.get(selectionIndex), outputIndex, input);
      }
    } else {
      for (int selectionIndex = 0; selectionIndex < selection.getValueCount(); selectionIndex++) {
        int outputIndex = outputOffset + selectionIndex;
        output.copyFromSafe(selection.get(selectionIndex), outputIndex, input);
      }
      output.setValueCount(outputRowCount);
    }
  }

  public static void takeTo(
      IntVector selectionVector, VectorSchemaRoot output, VectorSchemaRoot input) {
    if (output.getFieldVectors().size() != input.getFieldVectors().size()) {
      throw new IllegalArgumentException(
          "Output schema must have the same number of fields as input schema");
    }
    for (int i = 0; i < input.getFieldVectors().size(); i++) {
      takeTo(selectionVector, output.getFieldVectors().get(i), input.getFieldVectors().get(i));
    }
    output.setRowCount(output.getRowCount() + selectionVector.getValueCount());
  }

  @SuppressWarnings("unchecked")
  public static <OUTPUT extends FieldVector> OUTPUT take(
      BufferAllocator allocator, IntVector selection, OUTPUT input) {
    if (selection.getField().isNullable()) {
      throw new IllegalArgumentException("Selection vector must be not nullable");
    }
    TransferPair transferPair = input.getTransferPair(allocator);
    OUTPUT result = (OUTPUT) transferPair.getTo();
    result.setInitialCapacity(selection.getValueCount());
    takeTo(selection, result, input);
    return result;
  }

  public static VectorSchemaRoot take(
      BufferAllocator allocator, IntVector selectionVector, VectorSchemaRoot input) {
    List<FieldVector> results = new ArrayList<>();
    for (FieldVector fieldVector : input.getFieldVectors()) {
      results.add(take(allocator, selectionVector, fieldVector));
    }
    return new VectorSchemaRoot(results);
  }
}
