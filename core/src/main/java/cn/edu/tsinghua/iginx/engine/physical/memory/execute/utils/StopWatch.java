package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;

public class StopWatch implements AutoCloseable {

  private final ExecutorContext context;
  private final long startTimestamp;

  public StopWatch(ExecutorContext context) {
    this.context = context;
    this.startTimestamp = System.currentTimeMillis();
  }

  @Override
  public void close() {
    context.accumulateCpuTime(System.currentTimeMillis() - startTimestamp);
  }
}
