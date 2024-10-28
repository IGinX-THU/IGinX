package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.PhysicalExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;

import java.util.Objects;

public abstract class BinaryExecutor extends PhysicalExecutor {
  
  protected final BatchSchema leftSchema;
  protected final BatchSchema rightSchema;

  protected BinaryExecutor(ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema) {
    super(context);
    this.leftSchema = Objects.requireNonNull(leftSchema);
    this.rightSchema = Objects.requireNonNull(rightSchema);
  }

  public BatchSchema getLeftSchema() {
    return leftSchema;
  }

  public BatchSchema getRightSchema() {
    return rightSchema;
  }
}
