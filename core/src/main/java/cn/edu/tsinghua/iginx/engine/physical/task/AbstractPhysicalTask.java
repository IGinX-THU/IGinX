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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPhysicalTask<RESULT extends PhysicalCloseable>
    implements PhysicalTask<RESULT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPhysicalTask.class);

  private final RequestContext context;
  protected final ExecutorContext executorContext = new PhysicalTaskExecutorContext(this);

  private final TaskType type;

  private final List<Operator> operators;
  private PhysicalTask<?> followerTask;

  private final TaskMetrics metrics = new TaskMetrics();

  public AbstractPhysicalTask(TaskType type, List<Operator> operators, RequestContext context) {
    this.type = type;
    this.operators = operators;
    this.context = context;
  }

  @Override
  public RequestContext getContext() {
    return context;
  }

  @Override
  public TaskType getType() {
    return type;
  }

  @Override
  public List<Operator> getOperators() {
    return operators;
  }

  @Override
  public PhysicalTask<?> getFollowerTask() {
    return followerTask;
  }

  @Override
  public void setFollowerTask(PhysicalTask<?> task) {
    this.followerTask = task;
  }

  public TaskMetrics getMetrics() {
    return metrics;
  }

  @Override
  public String getInfo() {
    List<String> info =
        operators.stream()
            .map(op -> op.getType() + ":{" + op.getInfo() + "}")
            .collect(Collectors.toList());
    return String.join(",", info);
  }
}
