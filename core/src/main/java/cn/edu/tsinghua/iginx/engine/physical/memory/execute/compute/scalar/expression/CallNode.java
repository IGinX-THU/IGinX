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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

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
  protected FieldVector invokeImpl(BufferAllocator allocator, VectorSchemaRoot input)
      throws ComputeException {
    List<FieldVector> subResultList = new ArrayList<>();
    try {
      for (PhysicalExpression child : getChildren()) {
        subResultList.add(child.invoke(allocator, input));
      }
      try (VectorSchemaRoot args = new VectorSchemaRoot(subResultList)) {
        return function.invoke(allocator, args);
      }
    } finally {
      subResultList.forEach(FieldVector::close);
    }
  }
}
