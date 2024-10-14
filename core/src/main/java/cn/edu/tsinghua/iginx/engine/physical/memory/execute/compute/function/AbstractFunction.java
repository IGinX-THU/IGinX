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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

public abstract class AbstractFunction implements ScalarFunction {

  private final Arity arity;

  protected AbstractFunction(Arity arity) {
    this.arity = Objects.requireNonNull(arity);
  }

  @Override
  public Types.MinorType getResultType(ExecutorContext context, Types.MinorType... args) {
    checkArgument(args);
    return getReturnTypeImpl(context, args);
  }

  @Override
  public ValueVector invoke(
      ExecutorContext context, int rowCount, @WillNotClose ValueVector... args) {
    checkArgument(
        Arrays.stream(args).map(ValueVector::getMinorType).toArray(Types.MinorType[]::new));
    for (ValueVector arg : args) {
      if (arg.getValueCount() != rowCount) {
        throw new IllegalArgumentException(
            "Invalid number of rows for argument " + arg.getField() + " for function " + getName());
      }
    }
    return invokeImpl(context, rowCount, args);
  }

  @Override
  public String toString() {
    return getName();
  }

  private void checkArgument(Types.MinorType... args) {
    if (!arity.checkArity(args.length)) {
      throw new IllegalArgumentException(
          "Invalid number " + args.length + " of arguments for function " + getName());
    }
    for (int i = 0; i < args.length; i++) {
      if (!allowType(i, args[i])) {
        throw new IllegalArgumentException(
            "Invalid type " + args[i] + " of argument " + i + " for function " + getName());
      }
    }
  }

  protected abstract boolean allowType(int index, Types.MinorType type);

  protected abstract Types.MinorType getReturnTypeImpl(
      ExecutorContext context, Types.MinorType... args);

  protected abstract ValueVector invokeImpl(
      ExecutorContext context, int rowCount, ValueVector... args);
}
