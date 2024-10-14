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
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.Collections;
import javax.annotation.WillNotClose;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

public class FieldNode extends PhysicalExpression {

  private final int index;

  public FieldNode(int index) {
    super(Collections.emptyList());
    this.index = Preconditions.checkElementIndex(index, Integer.MAX_VALUE);
  }

  @Override
  public String getName() {
    return "field(" + index + ")";
  }

  @Override
  public Types.MinorType getResultType(ExecutorContext context, Types.MinorType... args) {
    return args[index];
  }

  @Override
  public ValueVector invoke(
      ExecutorContext context, int rowCount, @WillNotClose ValueVector... args) {
    return ValueVectors.slice(context.getAllocator(), args[index], rowCount);
  }
}
