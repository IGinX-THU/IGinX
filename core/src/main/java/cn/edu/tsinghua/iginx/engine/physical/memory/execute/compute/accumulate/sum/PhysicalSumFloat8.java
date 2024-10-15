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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.sum;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.Types;

public class PhysicalSumFloat8 extends PhysicalSum<Float8Vector, Double> {

  protected double result;

  public PhysicalSumFloat8(ExecutorContext context) {
    super(context, Types.MinorType.FLOAT8);
    result = 0;
  }

  @Override
  public String getName() {
    return "sum_float8";
  }

  @Override
  public Double evaluate() throws ComputeException {
    return result;
  }

  @Override
  protected void accumulateNullable(@WillNotClose Float8Vector vector) throws ComputeException {
    int valueCount = vector.getValueCount();
    for (int i = 0; i < valueCount; i++) {
      if (!vector.isNull(i)) {
        result += vector.get(i);
      }
    }
  }

  @Override
  protected void accumulateNotNull(@WillNotClose Float8Vector vector) throws ComputeException {
    int valueCount = vector.getValueCount();
    for (int i = 0; i < valueCount; i++) {
      result += vector.get(i);
    }
  }
}
