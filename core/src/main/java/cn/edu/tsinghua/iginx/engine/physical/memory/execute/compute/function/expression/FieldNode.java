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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.Collections;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FieldNode extends AbstractPhysicalExpression {

  private final int index;

  public FieldNode(int index) {
    this(index, null);
  }

  public FieldNode(int index, String alias) {
    super(alias, Collections.emptyList());
    this.index = index;
  }

  @Override
  public String getName() {
    return "field(" + index + ")";
  }

  @Override
  protected VectorSchemaRoot invokeImpl(ExecutorContext context, VectorSchemaRoot args)
      throws ComputeException {
    FieldVector slice =
        ValueVectors.slice(context.getAllocator(), args.getFieldVectors().get(index));
    return new VectorSchemaRoot(Collections.singletonList(slice));
  }
}
