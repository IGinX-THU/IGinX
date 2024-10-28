package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;

import javax.annotation.Nullable;
import javax.annotation.WillNotClose;

public abstract class StatefulBinaryExecutor extends BinaryExecutor {

  protected StatefulBinaryExecutor(ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema) {
    super(context, leftSchema, rightSchema);
  }

  protected boolean leftFinished = false;
  protected boolean rightFinished = false;

  public boolean consumeLeft(@WillNotClose @Nullable Batch batch) throws ComputeException {
    if (leftFinished) {
      throw new IllegalStateException("Left has been finished, cannot consume left again");
    }
    return consumeLeftInternal(batch);
  }


  public boolean consumeRight(@WillNotClose @Nullable Batch batch) throws ComputeException {
    if (rightFinished) {
      throw new IllegalStateException("Right has been finished, cannot consume right again");
    }
    return consumeRightInternal(batch);
  }

  public abstract boolean canProduce() throws ComputeException;

  public Batch produce() throws ComputeException {
    if (!canProduce()) {
      throw new IllegalStateException(getClass().getSimpleName() + " cannot produce");
    }
    return produceInternal();
  }

  protected abstract boolean consumeLeftInternal(@WillNotClose Batch batch) throws ComputeException;

  protected abstract boolean consumeRightInternal(@WillNotClose Batch batch) throws ComputeException;

  protected abstract Batch produceInternal() throws ComputeException;
}
