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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.cast;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.AbstractFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import java.util.Collections;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class Cast extends AbstractFunction {

  protected Cast(String from, String to) {
    super("cast_" + from + "_as_" + to, Arity.UNARY);
  }

  public abstract FieldVector evaluate(ExecutorContext context, ValueVector input);

  @Override
  protected VectorSchemaRoot invokeImpl(ExecutorContext context, VectorSchemaRoot args) {
    return new VectorSchemaRoot(Collections.singleton(evaluate(context, args.getVector(0))));
  }

  @Override
  public void close() {}
}
