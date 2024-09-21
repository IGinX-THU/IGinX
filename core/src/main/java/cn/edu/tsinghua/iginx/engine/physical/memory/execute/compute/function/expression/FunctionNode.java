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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

public class FunctionNode extends PhysicalExpression {

  private final ScalarFunction function;

  public FunctionNode(ScalarFunction function, PhysicalExpression... children) {
    this(function, Arrays.asList(children));
  }

  public FunctionNode(ScalarFunction function, List<PhysicalExpression> children) {
    super(children);
    this.function = Preconditions.checkNotNull(function);
  }

  @Override
  public String getName() {
    return function.getName()
        + getChildren().stream()
            .map(PhysicalExpression::getName)
            .collect(Collectors.joining(", ", "(", ")"));
  }

  @Override
  public Types.MinorType getResultType(ExecutorContext context, Types.MinorType... args) {
    Types.MinorType[] childrenTypes = new Types.MinorType[getChildren().size()];
    for (int i = 0; i < getChildren().size(); i++) {
      childrenTypes[i] = getChildren().get(i).getResultType(context, args);
    }
    return function.getResultType(context, childrenTypes);
  }

  @Override
  public ValueVector invoke(ExecutorContext context, int rowCount, ValueVector... args) {
    ValueVector[] childrenValues = new ValueVector[getChildren().size()];
    try {
      for (int i = 0; i < getChildren().size(); i++) {
        childrenValues[i] = getChildren().get(i).invoke(context, rowCount, args);
      }
      return function.invoke(context, rowCount, childrenValues);
    } finally {
      for (ValueVector childrenValue : childrenValues) {
        if (childrenValue != null) {
          childrenValue.close();
        }
      }
    }
  }
}
