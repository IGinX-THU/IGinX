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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.VectorSchemaRoots;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class FilterUnaryExecutor extends StatefulUnaryExecutor {

  private final ScalarExpression<BitVector> condition;
  private final List<ScalarExpression<?>> outputExpressions;
  private final Schema outputSchema;
  private final VectorSchemaRoot buffer;

  public FilterUnaryExecutor(
      ExecutorContext context,
      Schema inputSchema,
      ScalarExpression<BitVector> condition,
      List<? extends ScalarExpression<?>> outputExpressions)
      throws ComputeException {
    super(context, inputSchema, 1);
    this.condition = Objects.requireNonNull(condition);
    this.outputExpressions = new ArrayList<>(outputExpressions);
    this.outputSchema =
        ScalarExpressions.getOutputSchema(
            context.getAllocator(), outputExpressions, getInputSchema());
    this.buffer = VectorSchemaRoot.create(outputSchema, context.getAllocator());
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return outputSchema;
  }

  @Override
  public String getInfo() {
    return "Filter(" + condition + ")";
  }

  @Override
  public void close() throws ComputeException {
    buffer.close();
    super.close();
  }

  @Override
  protected void consumeUnchecked(VectorSchemaRoot batch) throws ComputeException {
    int expectedRowCount = context.getBatchRowCount();
    try (BitVector mask = ScalarExpressions.evaluateSafe(context.getAllocator(), condition, batch);
        IntVector selection = PhysicalFunctions.filter(context.getAllocator(), mask);
        VectorSchemaRoot toFiltered =
            ScalarExpressions.evaluateSafe(context.getAllocator(), outputExpressions, batch)) {
      int selectionOffset = 0;
      while (selectionOffset < selection.getValueCount()) {
        int toFillBufferRowCount =
            Math.min(
                expectedRowCount - buffer.getRowCount(),
                selection.getValueCount() - selectionOffset);
        toFillBufferRowCount = Math.max(toFillBufferRowCount, 0);
        try (IntVector selectionSlice =
            ValueVectors.slice(
                context.getAllocator(), selection, selectionOffset, toFillBufferRowCount)) {
          PhysicalFunctions.takeTo(selectionSlice, buffer, toFiltered);
        }
        if (buffer.getRowCount() >= expectedRowCount) {
          flush();
        }
        selectionOffset += toFillBufferRowCount;
      }
    }
  }

  @Override
  protected void consumeEnd() throws ComputeException {
    flush();
  }

  private void flush() {
    offerResult(VectorSchemaRoots.slice(context.getAllocator(), buffer));
    buffer.clear();
    buffer.getFieldVectors().forEach(vector -> vector.setInitialCapacity(buffer.getRowCount()));
    buffer.setRowCount(buffer.getRowCount());
  }
}
