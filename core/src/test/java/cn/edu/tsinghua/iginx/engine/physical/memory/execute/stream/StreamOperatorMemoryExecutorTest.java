package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.AbstractOperatorMemoryExecutorTest;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;

public class StreamOperatorMemoryExecutorTest extends AbstractOperatorMemoryExecutorTest {

  private final StreamOperatorMemoryExecutor executor;

  public StreamOperatorMemoryExecutorTest() {
    this.executor = StreamOperatorMemoryExecutor.getInstance();
  }

  @Override
  protected OperatorMemoryExecutor getExecutor() {
    return executor;
  }
}
