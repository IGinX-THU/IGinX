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
package cn.edu.tsinghua.iginx.resource;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantPool;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.memory.BufferAllocator;

public class ResourceSet implements AutoCloseable {

  private final BufferAllocator allocator;
  private final ConstantPool constantPool;

  public ResourceSet(@WillCloseWhenClosed BufferAllocator allocator) {
    this.allocator = allocator;
    this.constantPool = new ConstantPool(allocator);
  }

  @Override
  public void close() {
    constantPool.close();
    allocator.close();
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public ConstantPool getConstantPool() {
    return constantPool;
  }
}
