package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive.NaiveOperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.StreamOperatorMemoryExecutor;

public class OperatorMemoryExecutorFactory {

  private static final OperatorMemoryExecutorFactory INSTANCE = new OperatorMemoryExecutorFactory();

  private OperatorMemoryExecutorFactory() {}

  public OperatorMemoryExecutor getMemoryExecutor() {
    if (ConfigDescriptor.getInstance().getConfig().isUseStreamExecutor()) {
      return StreamOperatorMemoryExecutor.getInstance();
    }
    return NaiveOperatorMemoryExecutor.getInstance();
  }

  public static OperatorMemoryExecutorFactory getInstance() {
    return INSTANCE;
  }
}
