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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.WillClose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UnaryMemoryPhysicalTask<
        RESULT extends PhysicalCloseable, INPUT extends PhysicalCloseable>
    extends MemoryPhysicalTask<RESULT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnaryMemoryPhysicalTask.class);

  protected final PhysicalTask<INPUT> parentTask;

  public UnaryMemoryPhysicalTask(
      PhysicalTask<INPUT> parentTask, List<Operator> operators, RequestContext context) {
    super(TaskType.UnaryMemory, operators, context, 1);
    this.parentTask = parentTask;
  }

  public PhysicalTask<INPUT> getParentTask() {
    return parentTask;
  }

  @Override
  public TaskResult<RESULT> execute() {
    Future<TaskResult<INPUT>> future = parentTask.getResult();
    try (TaskResult<INPUT> parentResult = future.get()) {
      INPUT stream = parentResult.unwrap();
      RESULT result = compute(stream);
      return new TaskResult<>(result);
    } catch (PhysicalException e) {
      return new TaskResult<>(e);
    } catch (InterruptedException | ExecutionException e) {
      return new TaskResult<>(new PhysicalException(e));
    }
  }

  protected abstract RESULT compute(@WillClose INPUT previous) throws PhysicalException;

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    PhysicalTask<?> task = getParentTask();
    if (task != null) {
      task.accept(visitor);
    }
    visitor.leave();
  }
}
