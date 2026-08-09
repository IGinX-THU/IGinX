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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.sort.IndexSortExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArrowDictionaries;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.LazyBatch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import java.util.Objects;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Schema;

public class InnerBatchSortUnaryExecutor extends StatelessUnaryExecutor {

  private final IndexSortExpression indexSortExpression;

  public InnerBatchSortUnaryExecutor(
      ExecutorContext context, Schema inputSchema, IndexSortExpression indexSortExpression) {
    super(context, inputSchema);
    this.indexSortExpression = Objects.requireNonNull(indexSortExpression);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return getInputSchema();
  }

  @Override
  protected String getInfo() {
    return "SortBatch by " + indexSortExpression;
  }

  @Override
  public void close() throws ComputeException {}

  @Override
  public Batch computeImpl(Batch batch) throws ComputeException {
    try (IntVector sortedIndices =
        ScalarExpressionUtils.evaluate(
            context.getAllocator(),
            batch.getDictionaryProvider(),
            batch.getData(),
            null,
            indexSortExpression)) {
      try (LazyBatch result =
          ArrowDictionaries.select(
              context.getAllocator(),
              batch.getDictionaryProvider(),
              batch.getData(),
              sortedIndices)) {
        return Batch.of(
            VectorSchemaRoots.slice(context.getAllocator(), result.getData()),
            ArrowDictionaries.slice(
                context.getAllocator(),
                result.getDictionaryProvider(),
                result.getData().getSchema()));
      }
    }
  }
}
