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
package cn.edu.tsinghua.iginx.resource;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantPool;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.TaskResultMap;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.memory.BufferAllocator;

public class ResourceSet implements AutoCloseable {

  private final BufferAllocator allocator;
  private final ConstantPool constantPool;
  private final TaskResultMap taskResultMap;

  public ResourceSet(@WillCloseWhenClosed BufferAllocator allocator) {
    this.allocator = allocator;
    this.constantPool = new ConstantPool(allocator);
    this.taskResultMap = new TaskResultMap();
  }

  @Override
  public void close() throws PhysicalException {
    try {
      taskResultMap.close();
    } finally {
      constantPool.close();
      allocator.close();
    }
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public ConstantPool getConstantPool() {
    return constantPool;
  }

  public TaskResultMap getTaskResultMap() {
    return taskResultMap;
  }
}
