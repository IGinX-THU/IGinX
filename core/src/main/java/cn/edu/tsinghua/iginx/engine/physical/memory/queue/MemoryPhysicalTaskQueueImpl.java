package cn.edu.tsinghua.iginx.engine.physical.memory.queue;

import cn.edu.tsinghua.iginx.engine.physical.task.MemoryPhysicalTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryPhysicalTaskQueueImpl implements MemoryPhysicalTaskQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryPhysicalTaskQueueImpl.class);

  private final BlockingQueue<MemoryPhysicalTask> tasks = new LinkedBlockingQueue<>();

  @Override
  public boolean addTask(MemoryPhysicalTask memoryTask) {
    return tasks.add(memoryTask);
  }

  @Override
  public MemoryPhysicalTask getTask() {
    try {
      return tasks.take();
    } catch (Exception e) {
      LOGGER.error("encounter error when get memory task: ", e);
    }
    return null;
  }
}
