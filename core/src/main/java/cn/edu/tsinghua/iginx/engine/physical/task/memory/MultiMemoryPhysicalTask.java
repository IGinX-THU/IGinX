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

import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class MultiMemoryPhysicalTask<
        RESULT extends PhysicalCloseable, INPUT extends PhysicalCloseable>
    extends MemoryPhysicalTask<RESULT> {

  private final List<PhysicalTask<INPUT>> parentTasks;

  public MultiMemoryPhysicalTask(
      List<Operator> operators, RequestContext context, List<PhysicalTask<INPUT>> parentTasks) {
    super(TaskType.MultipleMemory, operators, context, parentTasks.size());
    this.parentTasks = Collections.unmodifiableList(new ArrayList<>(parentTasks));
  }

  public List<PhysicalTask<INPUT>> getParentTasks() {
    return parentTasks;
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    List<PhysicalTask<INPUT>> tasks = getParentTasks();
    for (PhysicalTask<?> task : tasks) {
      if (task != null) {
        task.accept(visitor);
      }
    }
    visitor.leave();
  }
}
