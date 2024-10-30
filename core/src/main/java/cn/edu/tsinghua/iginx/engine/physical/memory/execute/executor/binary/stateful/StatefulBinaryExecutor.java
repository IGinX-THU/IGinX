package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.WillNotClose;

public abstract class StatefulBinaryExecutor extends BinaryExecutor {

  protected StatefulBinaryExecutor(ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema) {
    super(context, leftSchema, rightSchema);
  }

  public abstract boolean needConsumeLeft() throws ComputeException;

  public abstract boolean needConsumeRight() throws ComputeException;

  /**
   * Consume a batch of data from the left child.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size less than the batch size
   * @throws ComputeException if an error occurs during consumption
   */
  public abstract void consumeLeft(@WillNotClose VectorSchemaRoot batch) throws ComputeException;

  /**
   * Consume a batch of data from the right child.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size less than the batch size
   * @return true if the executor needs to consume more data, false otherwise
   * @throws ComputeException if an error occurs during consumption
   */
  public abstract boolean consumeRight(@WillNotClose VectorSchemaRoot batch) throws ComputeException;

  /**
   * Produce the result of the computation.
   *
   * @return the result of the computation. Empty if executor needs to consume more data.
   */
  public abstract VectorSchemaRoot produce() throws ComputeException;

}
