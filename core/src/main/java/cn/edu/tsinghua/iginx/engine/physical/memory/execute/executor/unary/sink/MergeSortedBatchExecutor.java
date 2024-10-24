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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;

public class MergeSortedBatchExecutor extends UnarySinkExecutor {

  private final List<VectorSchemaRoot> batches = new ArrayList<>();

  public MergeSortedBatchExecutor(ExecutorContext context, BatchSchema inputSchema) {
    super(context, inputSchema);
  }

  @Override
  protected void internalConsume(Batch batch) throws ComputeException {
    batches.add(batch.raw().slice(0));
  }

  @Override
  protected void internalFinish() throws ComputeException {
    if (batches.isEmpty()) {
      throw new ComputeException("Cannot merge empty batches");
    } else if (batches.size() != 1) {
      throw new ComputeException("Merging multiple batches is not implemented");
    }
    canProduce = true;
  }

  private boolean canProduce = false;

  @Override
  public boolean canProduce() throws ComputeException {
    return canProduce;
  }

  @Override
  protected Batch internalProduce() throws ComputeException {
    canProduce = false;
    return new Batch(batches.get(0).slice(0));
  }

  @Override
  public BatchSchema getOutputSchema() throws ComputeException {
    if (batches.isEmpty()) {
      throw new ComputeException("Cannot get output schema from empty batches");
    }
    return BatchSchema.of(batches.get(0).getSchema());
  }

  @Override
  protected String getInfo() {
    return "MergeSortedBatch";
  }

  @Override
  public void close() throws ComputeException {
    batches.forEach(VectorSchemaRoot::close);
    batches.clear();
  }
}
