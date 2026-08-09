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
package cn.edu.tsinghua.iginx.engine.physical.task.memory;

import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.Collections;
import java.util.Objects;

public abstract class SourceMemoryPhysicalTask extends MemoryPhysicalTask<BatchStream> {

  private final Object info;

  public SourceMemoryPhysicalTask(RequestContext context, Object info) {
    super(TaskType.SourceMemory, Collections.emptyList(), context, 0);
    this.info = Objects.requireNonNull(info);
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);
    visitor.leave();
  }

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }

  @Override
  public String getInfo() {
    return String.valueOf(info);
  }

  @Override
  public boolean notifyParentReady() {
    return true;
  }
}
