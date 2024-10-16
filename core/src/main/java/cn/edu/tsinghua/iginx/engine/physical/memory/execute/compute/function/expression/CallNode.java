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
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CallNode extends AbstractPhysicalExpression {

  private final ScalarFunction function;

  public CallNode(ScalarFunction function, PhysicalExpression... children) {
    this(function, null, children);
  }

  public CallNode(ScalarFunction function, @Nullable String alias, PhysicalExpression... children) {
    this(function, alias, Arrays.asList(children));
  }

  public CallNode(
      ScalarFunction function, @Nullable String alias, List<PhysicalExpression> children) {
    super(alias, children);
    this.function = Preconditions.checkNotNull(function);
  }

  @Override
  public String getName() {
    return function.getName()
        + getChildren().stream()
        .map(PhysicalExpression::toString)
        .collect(Collectors.joining(",", "(", ")"));
  }

  @Override
  protected VectorSchemaRoot invokeImpl(ExecutorContext context, VectorSchemaRoot args)
      throws ComputeException {
    List<FieldVector> subResultList = new ArrayList<>();
    try {
      for (PhysicalExpression child : getChildren()) {
        subResultList.add(child.evaluate(context, args));
      }
    } catch (ComputeException e) {
      subResultList.forEach(FieldVector::close);
      throw e;
    }
    try (VectorSchemaRoot expressionArgs = new VectorSchemaRoot(subResultList)) {
      return function.invoke(context, expressionArgs);
    }
  }
}
