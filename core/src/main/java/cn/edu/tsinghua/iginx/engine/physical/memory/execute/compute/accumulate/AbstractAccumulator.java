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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.ComputeException;
import java.util.Objects;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

public abstract class AbstractAccumulator<IN extends ValueVector, OUT> implements Accumulator {

  protected final ExecutorContext context;
  protected final Types.MinorType type;

  protected AbstractAccumulator(ExecutorContext context, Types.MinorType type) {
    this.context = Objects.requireNonNull(context);
    this.type = Objects.requireNonNull(type);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void accumulate(@WillNotClose ValueVector vector) throws ComputeException {
    if (vector.getMinorType() != type) {
      throw new ComputeException(
          getClass().getSimpleName()
              + " only supports "
              + type
              + ", but got "
              + vector.getMinorType());
    }
    if (vector.getValueCount() == 0) {
      return;
    }
    accumulateTyped((IN) vector);
  }

  public abstract void accumulateTyped(@WillNotClose IN vector) throws ComputeException;

  @Override
  public abstract OUT evaluate() throws ComputeException;
}
