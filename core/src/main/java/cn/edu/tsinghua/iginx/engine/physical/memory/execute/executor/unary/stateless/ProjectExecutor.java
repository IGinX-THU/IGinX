/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class ProjectExecutor extends StatelessUnaryExecutor {

  protected final List<ScalarExpression<?>> expressions;
  protected Schema outputSchema;

  public ProjectExecutor(
      ExecutorContext context,
      Schema inputSchema,
      List<? extends ScalarExpression<?>> expressions) {
    this(context, inputSchema, expressions, false);
  }

  public ProjectExecutor(
      ExecutorContext context,
      Schema inputSchema,
      List<? extends ScalarExpression<?>> expressions,
      boolean empty) {
    super(context, inputSchema, empty);
    this.expressions = new ArrayList<>(expressions);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      outputSchema =
          ScalarExpressionUtils.getOutputSchema(
              context.getAllocator(), expressions, getInputSchema());
    }
    return outputSchema;
  }

  @Override
  public String getInfo() {
    return "Project" + expressions;
  }

  @Override
  public void close() {}

  @Override
  public Batch computeImpl(Batch batch) throws ComputeException {
    for (long id : batch.getDictionaryProvider().getDictionaryIds()) {
      Preconditions.checkState(
          !batch
              .getDictionaryProvider()
              .lookup(id)
              .getVector()
              .getMinorType()
              .getType()
              .isComplex());
    }
    try (VectorSchemaRoot result =
        ScalarExpressionUtils.evaluate(
            context.getAllocator(),
            batch.getDictionaryProvider(),
            batch.getData(),
            null,
            expressions)) {
      return batch.sliceWith(
          context.getAllocator(), PhysicalFunctions.unnest(context.getAllocator(), result));
    }
  }
}
