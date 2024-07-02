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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 目前专门用于 CombineNonQuery 操作符 */
public class MultipleMemoryPhysicalTask extends MemoryPhysicalTask {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(MultipleMemoryPhysicalTask.class);

  private final List<PhysicalTask> parentTasks;

  public MultipleMemoryPhysicalTask(
      List<Operator> operators, List<PhysicalTask> parentTasks, RequestContext context) {
    super(TaskType.MultipleMemory, operators, context);
    this.parentTasks = parentTasks;
  }

  public List<PhysicalTask> getParentTasks() {
    return parentTasks;
  }

  @Override
  public TaskExecuteResult execute() {
    List<Operator> operators = getOperators();
    if (operators.size() != 1) {
      return new TaskExecuteResult(
          new PhysicalException("unexpected multiple memory physical task"));
    }
    Operator operator = operators.get(0);
    if (operator.getType() != OperatorType.CombineNonQuery) {
      return new TaskExecuteResult(
          new PhysicalException("unexpected multiple memory physical task"));
    }
    if (getFollowerTask() != null) {
      return new TaskExecuteResult(
          new PhysicalException("multiple memory physical task shouldn't have follower task"));
    }
    List<PhysicalException> exceptions = new ArrayList<>();
    for (PhysicalTask parentTask : parentTasks) {
      PhysicalException exception = parentTask.getResult().getException();
      if (exception != null) {
        exceptions.add(exception);
      }
    }
    if (exceptions.size() != 0) {
      StringBuilder message = new StringBuilder("some sub-task execute failure, details: ");
      for (PhysicalException exception : exceptions) {
        message.append(exception.getMessage());
      }
      return new TaskExecuteResult(new PhysicalTaskExecuteFailureException(message.toString()));
    }
    return new TaskExecuteResult();
  }

  @Override
  public boolean notifyParentReady() {
    return parentReadyCount.incrementAndGet() == parentTasks.size();
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    List<PhysicalTask> tasks = getParentTasks();
    for (PhysicalTask task : tasks) {
      if (task != null) {
        task.accept(visitor);
      }
    }
    visitor.leave();
  }
}
