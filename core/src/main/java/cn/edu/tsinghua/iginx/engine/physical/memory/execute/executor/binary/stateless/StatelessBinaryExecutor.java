package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateless;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;

import javax.annotation.WillClose;

public abstract class StatelessBinaryExecutor extends BinaryExecutor {

  protected StatelessBinaryExecutor(ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema) {
    super(context, leftSchema, rightSchema);
  }

  private BatchSchema outputSchema;

  @Override
  public BatchSchema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      try (Batch emptyLeftBatch = leftSchema.emptyBatch(context.getAllocator());
           Batch emptyRightBatch = rightSchema.emptyBatch(context.getAllocator());
           Batch producedBatch = compute(emptyLeftBatch, emptyRightBatch)) {
        outputSchema = producedBatch.getSchema();
      }
    }
    return outputSchema;
  }

  public Batch compute(@WillClose Batch leftBatch, @WillClose Batch rightBatch) throws ComputeException {
    try (StopWatch watch = new StopWatch(context::addPipelineComputeTime)) {
      try (Batch producedBatch = internalCompute(leftBatch, rightBatch)) {
        context.addProducedRowNumber(producedBatch.getRowCount());
        return producedBatch;
      }
    } finally {
      leftBatch.close();
      rightBatch.close();
    }
  }

  protected abstract Batch internalCompute(@WillClose Batch leftBatch, @WillClose Batch rightBatch) throws ComputeException;
}
