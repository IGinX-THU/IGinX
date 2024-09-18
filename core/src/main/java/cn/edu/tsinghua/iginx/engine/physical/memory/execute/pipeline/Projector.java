package cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;

public class Projector extends PipelineExecutor {

  @Override
  public String getDescription() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BatchSchema getOutputSchema(ExecutorContext context, BatchSchema inputSchema) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  protected Batch computeWithoutTimer(ExecutorContext context, Batch batch) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
