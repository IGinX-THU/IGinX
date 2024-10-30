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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.selecting;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ComplexFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

public class CaseWhen extends ComplexFunction<FieldVector> {

  private static final String name = "casewhen";

  public CaseWhen() {
    super(name);
  }

  /**
   * @param allocator
   * @param inputs List of conditions and values. [condition1, value1, condition2, value2, ...,
   *     valueElse]
   */
  @Override
  public FieldVector evaluate(
      BufferAllocator allocator, List<org.apache.arrow.vector.FieldVector> inputs)
      throws ComputeException {
    int rowCount = inputs.get(0).getValueCount();
    Types.MinorType minorType = inputs.get(1).getMinorType();
    int fieldIndex = 0;
    // 三个检查：奇数位bool & 偶数位类型相同 & 每列元素数量相等
    for (; fieldIndex < inputs.size() - 1; fieldIndex += 2) {
      if (fieldIndex % 2 == 0 && !(inputs.get(fieldIndex) instanceof BitVector)) {
        throw new ComputeException(name + " expects a BitVector but got " + inputs.get(fieldIndex));
      } else if (fieldIndex % 2 != 0 && inputs.get(fieldIndex).getMinorType() != minorType) {
        throw new ComputeException(
            name + " expects a " + minorType + " but got " + inputs.get(fieldIndex));
      }
      if (inputs.get(fieldIndex).getValueCount() != rowCount) {
        throw new ComputeException(
            name + " expects " + rowCount + " elements but got " + inputs.get(fieldIndex));
      }
    }
    if (inputs.size() % 2 != 0 && inputs.get(fieldIndex).getMinorType() != minorType) {
      throw new ComputeException(
          name + " expects a " + minorType + " but got " + inputs.get(fieldIndex));
    }

    FieldVector result = ValueVectors.create(allocator, minorType, rowCount);
    evaluateGeneric(result, inputs, minorType);
    return result;
  }

  private <T extends FieldVector> void evaluateGeneric(
      T result, List<FieldVector> inputs, Types.MinorType minorType) throws ComputeException {
    int rowCount = inputs.get(0).getValueCount();
    boolean matched;
    int fieldIndex;

    for (int row = 0; row < rowCount; row++) {
      matched = false;

      for (fieldIndex = 0; fieldIndex < inputs.size() - 1; fieldIndex += 2) {
        if (((BitVector) inputs.get(fieldIndex)).get(row) == 1) {
          matched = true;
          setValue(result, row, inputs.get(fieldIndex + 1), minorType);
          break;
        }
      }

      if (!matched) {
        if (inputs.size() % 2 == 0) {
          throw new ComputeException("No default branch provided in " + name);
        } else {
          setValue(result, row, inputs.get(fieldIndex), minorType);
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
