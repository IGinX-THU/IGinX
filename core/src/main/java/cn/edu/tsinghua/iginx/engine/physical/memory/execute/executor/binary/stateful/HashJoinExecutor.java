package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class HashJoinExecutor extends StatefulBinaryExecutor {

  public HashJoinExecutor(ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema) {
    super(context, leftSchema, rightSchema);
  }

  @Override
  public boolean needConsumeLeft() throws ComputeException {
    return false;
  }

  @Override
  public boolean needConsumeRight() throws ComputeException {
    return false;
  }

  @Override
  public boolean canProduce() throws ComputeException {
    return false;
  }

  @Override
  public void consumeLeft(VectorSchemaRoot batch) throws ComputeException {

  }

  @Override
  public boolean consumeRight(VectorSchemaRoot batch) throws ComputeException {
    return false;
  }


  @Override
  public VectorSchemaRoot produce() throws ComputeException {
    return null;
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return null;
  }

  @Override
  protected String getInfo() {
    return "";
  }

  @Override
  public void close() throws ComputeException {

  }
}
