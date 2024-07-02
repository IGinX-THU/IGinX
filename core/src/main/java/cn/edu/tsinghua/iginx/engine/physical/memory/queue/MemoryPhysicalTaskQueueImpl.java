/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
