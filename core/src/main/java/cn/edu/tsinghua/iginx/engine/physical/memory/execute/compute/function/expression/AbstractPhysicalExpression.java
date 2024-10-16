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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.WillNotClose;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractPhysicalExpression implements PhysicalExpression {

  private final List<PhysicalExpression> children;
  private final String alias;

  protected AbstractPhysicalExpression(String alias, List<PhysicalExpression> children) {
    this.alias = alias;
    this.children = Collections.unmodifiableList(children);
  }

  @Override
  public List<PhysicalExpression> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    return getName() + (alias == null ? "" : " as " + alias);
  }

  public VectorSchemaRoot invoke(ExecutorContext context, @WillNotClose VectorSchemaRoot args)
      throws ComputeException {
    try (VectorSchemaRoot result = invokeImpl(context, args)) {
      List<FieldVector> vectorsWithAlias = new ArrayList<>();
      for (FieldVector vector : result.getFieldVectors()) {
        if (alias == null) {
          vectorsWithAlias.add(ValueVectors.transfer(context.getAllocator(), vector));
        } else {
          vectorsWithAlias.add(ValueVectors.transfer(context.getAllocator(), vector, alias));
        }
      }
      return new VectorSchemaRoot(vectorsWithAlias);
    }
  }

  protected abstract VectorSchemaRoot invokeImpl(
      ExecutorContext context, @WillNotClose VectorSchemaRoot args) throws ComputeException;

  @Override
  public void close() {
    for (PhysicalExpression child : children) {
      child.close();
    }
  }
}
