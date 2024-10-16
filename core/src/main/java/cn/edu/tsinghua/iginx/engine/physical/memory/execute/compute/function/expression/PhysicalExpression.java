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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.WillNotClose;
import java.util.List;

public interface PhysicalExpression extends ScalarFunction {

  List<PhysicalExpression> getChildren();

  default FieldVector evaluate(ExecutorContext context, @WillNotClose VectorSchemaRoot args)
      throws ComputeException {
    try (VectorSchemaRoot result = invoke(context, args)) {
      if (result.getFieldVectors().size() != 1) {
        throw new ComputeException("Physical expression " + this + " returned multiple columns: " + result.getSchema());
      }
      return ValueVectors.transfer(context.getAllocator(), result.getFieldVectors().get(0));
    }
  }

  default <T extends FieldVector> T evaluate(ExecutorContext context, @WillNotClose VectorSchemaRoot args, Class<T> clazz)
      throws ComputeException {
    FieldVector vector = evaluate(context, args);
    if (!clazz.isInstance(vector)) {
      vector.close();
      throw new ComputeException("Physical expression " + this + " returned not a " + clazz.getSimpleName() + " but a " + vector.getClass().getSimpleName());
    } else {
      return clazz.cast(vector);
    }
  }
}
