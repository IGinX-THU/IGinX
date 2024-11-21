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

package cn.edu.tsinghua.iginx.engine.physical.task.memory;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 目前专门用于 CombineNonQuery 操作符 */
public class MultipleMemoryPhysicalTask extends MemoryPhysicalTask<BatchStream> {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(MultipleMemoryPhysicalTask.class);

  private final List<PhysicalTask<?>> parentTasks;

  public MultipleMemoryPhysicalTask(
      List<Operator> operators, List<PhysicalTask<?>> parentTasks, RequestContext context) {
    super(TaskType.MultipleMemory, operators, context, parentTasks.size());
    this.parentTasks = parentTasks;
  }

  public List<PhysicalTask<?>> getParentTasks() {
    return parentTasks;
  }

  @Override
  public TaskResult<BatchStream> execute() {
    List<Operator> operators = getOperators();
    if (operators.size() != 1) {
      return new TaskResult<>(new PhysicalException("unexpected multiple memory physical task"));
    }
    Operator operator = operators.get(0);
    if (operator.getType() != OperatorType.CombineNonQuery) {
      return new TaskResult<>(new PhysicalException("unexpected multiple memory physical task"));
    }
    if (getFollowerTask() != null) {
      return new TaskResult<>(
          new PhysicalException("multiple memory physical task shouldn't have follower task"));
    }
    PhysicalException exception = null;
    for (PhysicalTask<?> parentTask : parentTasks) {
      try {
        parentTask.getResult().get().close();
      } catch (PhysicalException | ExecutionException | InterruptedException e) {
        if (exception == null) {
          if (e instanceof PhysicalException) {
            exception = (PhysicalException) e;
          } else {
            exception = new PhysicalException(e);
          }
        } else {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      return new TaskResult<>(exception);
    }
    return new TaskResult<>(BatchStreams.empty());
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    List<PhysicalTask<?>> tasks = getParentTasks();
    for (PhysicalTask<?> task : tasks) {
      if (task != null) {
        task.accept(visitor);
      }
    }
    visitor.leave();
  }

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }
}
