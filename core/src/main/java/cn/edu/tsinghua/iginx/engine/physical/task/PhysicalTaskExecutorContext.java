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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantPool;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;

public class PhysicalTaskExecutorContext implements ExecutorContext {

  private final PhysicalTask task;

  public PhysicalTaskExecutorContext(PhysicalTask task) {
    this.task = Objects.requireNonNull(task);
  }

  @Override
  public BufferAllocator getAllocator() {
    return task.getContext().getAllocator();
  }

  @Override
  public ConstantPool getConstantPool() {
    return task.getContext().getConstantPool();
  }

  @Override
  public int getBatchRowCount() {
    return task.getContext().getBatchRowCount();
  }

  @Override
  public void addWarningMessage(String message) {
    task.getContext().addWarningMessage(message);
  }

  @Override
  public int getGroupByInitialGroupBufferCapacity() {
    return task.getContext().getGroupByInitialGroupBufferCapacity();
  }
}
