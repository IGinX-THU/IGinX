package cn.edu.tsinghua.iginx.engine.physical.task;

import org.apache.arrow.memory.BufferAllocator;

public interface TaskContext {

  BufferAllocator getAllocator();

  void addWarningMessage(String message);
}
