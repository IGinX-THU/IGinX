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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.selecting;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.AbstractScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

public class CaseWhen extends AbstractScalarFunction<FieldVector> {

  private static final String name = "casewhen";

  public CaseWhen() {
    super(name, new Arity(Arity.COMPLEX.getArity(), true));
  }

  @Override
  protected FieldVector invokeImpl(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    if (selection == null) {
      return invokeImpl(allocator, input);
    }
    try (VectorSchemaRoot selected = PhysicalFunctions.take(allocator, selection, input)) {
      return invokeImpl(allocator, selected);
    }
  }

  protected FieldVector invokeImpl(BufferAllocator allocator, VectorSchemaRoot input)
      throws ComputeException {
    validateInput(input.getFieldVectors().toArray(new FieldVector[0]));
    return evaluate(allocator, input.getFieldVectors().toArray(new FieldVector[0]));
  }

  public void validateInput(org.apache.arrow.vector.FieldVector[] inputs) throws ComputeException {
    int rowCount = inputs[0].getValueCount();
    Types.MinorType minorType = inputs[1].getMinorType();
    int fieldIndex = 0;
    // 三个检查：奇数位bool & 偶数位类型相同 & 每列元素数量相等，最后一列一定是值列，留到最后
    for (; fieldIndex < inputs.length - 1; fieldIndex++) {
      if (fieldIndex % 2 == 0 && !(inputs[fieldIndex] instanceof BitVector)) {
        throw new ComputeException(name + " expects a BitVector but got " + inputs[fieldIndex]);
      } else if (fieldIndex % 2 != 0 && inputs[fieldIndex].getMinorType() != minorType) {
        throw new ComputeException(
            name + " expects a " + minorType + " but got " + inputs[fieldIndex]);
      }
      if (inputs[fieldIndex].getValueCount() != rowCount) {
        throw new ComputeException(
            name + " expects " + rowCount + " elements but got " + inputs[fieldIndex]);
      }
    }
    // 最后一列
    if (inputs[fieldIndex].getMinorType() != minorType) {
      throw new ComputeException(
          name + " expects a " + minorType + " but got " + inputs[fieldIndex]);
    }
    if (inputs[fieldIndex].getValueCount() != rowCount) {
      throw new ComputeException(
          name + " expects " + rowCount + " elements but got " + inputs[fieldIndex]);
    }
  }

  /**
   * @param allocator
   * @param inputs List of conditions and values. [condition1, value1, condition2, value2, ...,
   *     valueElse]
   */
  public FieldVector evaluate(
      BufferAllocator allocator, org.apache.arrow.vector.FieldVector[] inputs)
      throws ComputeException {
    int rowCount = inputs[0].getValueCount();
    Types.MinorType minorType = inputs[1].getMinorType();
    FieldVector result = ValueVectors.create(allocator, minorType, rowCount);
    evaluateGeneric(result, inputs, minorType);
    return result;
  }

  private <T extends FieldVector> void evaluateGeneric(
      T result, FieldVector[] inputs, Types.MinorType minorType) throws ComputeException {
    int rowCount = inputs[0].getValueCount();
    boolean matched;
    int fieldIndex;

    for (int row = 0; row < rowCount; row++) {
      matched = false;

      for (fieldIndex = 0; fieldIndex < inputs.length - 1; fieldIndex += 2) {
        if (((BitVector) inputs[fieldIndex]).get(row) != 0) {
          matched = true;
          setValue(result, row, inputs[fieldIndex + 1], minorType);
          break;
        }
      }

      if (!matched) {
        if (inputs.length % 2 == 0) {
          // 没有else条件且没有匹配
          result.setNull(row);
        } else {
          setValue(result, row, inputs[fieldIndex], minorType);
        }
      }
    }
  }

  private <T extends FieldVector> void setValue(
      T result, int row, FieldVector valueVector, Types.MinorType minorType) {
    switch (minorType) {
      case INT:
        ((IntVector) result).setSafe(row, ((IntVector) valueVector).get(row));
        break;
      case BIGINT:
        ((BigIntVector) result).setSafe(row, ((BigIntVector) valueVector).get(row));
        break;
      case BIT:
        ((BitVector) result).setSafe(row, ((BitVector) valueVector).get(row));
        break;
      case FLOAT4:
        ((Float4Vector) result).setSafe(row, ((Float4Vector) valueVector).get(row));
        break;
      case FLOAT8:
        ((Float8Vector) result).setSafe(row, ((Float8Vector) valueVector).get(row));
        break;
      case VARBINARY:
        ((VarBinaryVector) result).setSafe(row, ((VarBinaryVector) valueVector).get(row));
        break;
      default:
        throw new IllegalArgumentException("Unsupported MinorType: " + minorType);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof CaseWhen;
  }
}
