/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.queue;

import cn.edu.tsinghua.iginx.engine.physical.task.memory.MemoryPhysicalTask;

public interface MemoryPhysicalTaskQueue {

  /**
   * 向任务队列中添加任务
   *
   * @param memoryTask 内存任务
   * @return 是否成功添加任务
   */
  boolean addTask(MemoryPhysicalTask<?> memoryTask);

  /**
   * 如果当前队列中不含未执行的计划，则该方法会阻塞。
   *
   * @return 距今最久的未执行的计划
   */
  MemoryPhysicalTask<?> getTask();
}
