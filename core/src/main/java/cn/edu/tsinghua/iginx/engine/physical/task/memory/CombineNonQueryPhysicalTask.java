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
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 目前专门用于 CombineNonQuery 操作符 */
public class CombineNonQueryPhysicalTask extends MultiMemoryPhysicalTask<BatchStream, RowStream> {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(CombineNonQueryPhysicalTask.class);

  public CombineNonQueryPhysicalTask(
      List<Operator> operators, List<PhysicalTask<RowStream>> parentTasks, RequestContext context) {
    super(operators, context, parentTasks);
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
    for (PhysicalTask<?> parentTask : getParentTasks()) {
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
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }
}
