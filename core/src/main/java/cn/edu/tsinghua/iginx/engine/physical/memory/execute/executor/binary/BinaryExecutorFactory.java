package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;

public interface BinaryExecutorFactory<T extends BinaryExecutor> {

  T initialize(ExecutorContext context, BatchSchema inputSchema) throws ComputeException;
}
