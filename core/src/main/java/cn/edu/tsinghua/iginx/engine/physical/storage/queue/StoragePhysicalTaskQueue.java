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
