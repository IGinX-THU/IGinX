package cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.PhysicalExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import javax.annotation.WillClose;
import javax.annotation.WillNotClose;
import jdk.nashorn.internal.ir.annotations.Immutable;

@Immutable
public abstract class PipelineExecutor implements PhysicalExecutor {

  public abstract BatchSchema getOutputSchema(ExecutorContext context, BatchSchema inputSchema)
      throws PhysicalException;

  public Batch compute(ExecutorContext context, @WillClose Batch batch) throws PhysicalException {
    try {
      long start = System.currentTimeMillis();
      Batch producedBatch = computeWithoutTimer(context, batch);
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      context.accumulateCpuTime(elapsed);
      return producedBatch;
    } finally {
      batch.close();
    }
  }

  protected abstract Batch computeWithoutTimer(ExecutorContext context, @WillNotClose Batch batch)
      throws PhysicalException;
}
