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

import cn.edu.tsinghua.iginx.engine.physical.task.AbstractPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MemoryPhysicalTask<RESULT extends PhysicalCloseable>
    extends AbstractPhysicalTask<RESULT> {

  protected final AtomicInteger parentReadyCount;

  public MemoryPhysicalTask(
      TaskType type, List<Operator> operators, RequestContext context, int children) {
    super(type, operators, context);
    parentReadyCount = new AtomicInteger(children);
  }

  public abstract TaskResult<RESULT> execute(); // 在 parent 都完成执行后，可以执行该任务

  // 通知当前任务的某个父节点已经完成，该方法会返回 boolean 值，表示当前的任务是否可以执行
  public boolean notifyParentReady() {
    return parentReadyCount.decrementAndGet() == 0;
  }
}
