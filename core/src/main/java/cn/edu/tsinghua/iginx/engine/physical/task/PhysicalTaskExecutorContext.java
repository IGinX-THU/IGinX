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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantPool;
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
  public int groupByInitialGroupBufferCapacity() {
    return task.getContext().groupByInitialGroupBufferCapacity();
  }
}
