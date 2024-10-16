package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import org.apache.arrow.vector.VectorSchemaRoot;

public class Less implements ScalarFunction {
  @Override
  public String getName() {
    return "";
  }

  @Override
  public VectorSchemaRoot invoke(ExecutorContext context, VectorSchemaRoot args) throws ComputeException {
    return null;
  }


  @Override
  public void close() {

  }
}
