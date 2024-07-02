package cn.edu.tsinghua.iginx.engine.physical.memory.queue;

import cn.edu.tsinghua.iginx.engine.physical.task.MemoryPhysicalTask;

public interface MemoryPhysicalTaskQueue {

  /**
   * 向任务队列中添加任务
   *
   * @param memoryTask 内存任务
   * @return 是否成功添加任务
   */
  boolean addTask(MemoryPhysicalTask memoryTask);

  /**
   * 如果当前队列中不含未执行的计划，则该方法会阻塞。
   *
   * @return 距今最久的未执行的计划
   */
  MemoryPhysicalTask getTask();
}
