package cn.edu.tsinghua.iginx.engine.physical.storage.queue;

import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoragePhysicalTaskQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(StoragePhysicalTaskQueue.class);

  private final BlockingQueue<StoragePhysicalTask> tasks;

  public StoragePhysicalTaskQueue() {
    tasks = new LinkedBlockingQueue<>();
  }

  public void addTask(StoragePhysicalTask task) {
    try {
      tasks.put(task);
    } catch (InterruptedException e) {
      LOGGER.error("add task to physical task queue error: ", e);
    }
  }

  public StoragePhysicalTask getTask() {
    try {
      return tasks.take();
    } catch (Exception e) {
      LOGGER.error("encounter error when get memory task: ", e);
    }
    return null;
  }
}
