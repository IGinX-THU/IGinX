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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MemoryPhysicalTask extends AbstractPhysicalTask {

  protected AtomicInteger parentReadyCount;

  public MemoryPhysicalTask(TaskType type, List<Operator> operators, RequestContext context) {
    super(type, operators, context);
    parentReadyCount = new AtomicInteger(0);
  }

  public abstract TaskExecuteResult execute(); // 在 parent 都完成执行后，可以执行该任务

  public abstract boolean notifyParentReady(); // 通知当前任务的某个父节点已经完成，该方法会返回 boolean 值，表示当前的任务是否可以执行
}
