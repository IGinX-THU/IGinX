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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.AbstractAccumulator;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

public abstract class PhysicalSum<IN extends ValueVector, OUT>
    extends AbstractAccumulator<IN, OUT> {

  protected PhysicalSum(ExecutorContext context, Types.MinorType type) {
    super(context, type);
  }

  @Override
  public void accumulateTyped(@WillNotClose IN vector) throws ComputeException {
    int nullCount = vector.getNullCount();
    if (nullCount == 0) {
      accumulateNotNull(vector);
    } else if (nullCount < vector.getValueCount()) {
      accumulateNullable(vector);
    }
  }

  protected abstract void accumulateNullable(@WillNotClose IN vector) throws ComputeException;

  protected abstract void accumulateNotNull(@WillNotClose IN vector) throws ComputeException;

  @Override
  public void close() {}
}
