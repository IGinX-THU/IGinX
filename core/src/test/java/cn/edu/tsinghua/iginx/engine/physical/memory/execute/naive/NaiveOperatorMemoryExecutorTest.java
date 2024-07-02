package cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.AbstractOperatorMemoryExecutorTest;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;

public class NaiveOperatorMemoryExecutorTest extends AbstractOperatorMemoryExecutorTest {

  private final NaiveOperatorMemoryExecutor executor;

  public NaiveOperatorMemoryExecutorTest() {
    executor = NaiveOperatorMemoryExecutor.getInstance();
  }

  @Override
  protected OperatorMemoryExecutor getExecutor() {
    return executor;
  }
}
