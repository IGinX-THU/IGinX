package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import org.apache.arrow.memory.BufferAllocator;

public interface ExecutorContext {

  BufferAllocator getAllocator();

  void addWarningMessage(String message);

  void accumulateCpuTime(long millis);

  void accumulateProducedRows(long rows);
}
