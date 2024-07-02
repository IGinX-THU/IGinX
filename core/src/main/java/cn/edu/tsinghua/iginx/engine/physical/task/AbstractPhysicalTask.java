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
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPhysicalTask implements PhysicalTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPhysicalTask.class);

  private final RequestContext context;

  private final TaskType type;

  private final List<Operator> operators;
  private final CountDownLatch resultLatch = new CountDownLatch(1);
  private PhysicalTask followerTask;
  private TaskExecuteResult result;

  private int affectRows = 0;

  private long span = 0;

  public AbstractPhysicalTask(TaskType type, List<Operator> operators, RequestContext context) {
    this.type = type;
    this.operators = operators;
    this.context = context;
  }

  public RequestContext getContext() {
    return context;
  }

  public long getSessionId() {
    return context.getSessionId();
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
  public PhysicalTask getFollowerTask() {
    return followerTask;
  }

  @Override
  public void setFollowerTask(PhysicalTask task) {
    this.followerTask = task;
  }

  @Override
  public TaskExecuteResult getResult() {
    try {
      this.resultLatch.await();
    } catch (InterruptedException e) {
      LOGGER.error("unexpected interrupted when get result: ", e);
    }
    return result;
  }

  @Override
  public void setResult(TaskExecuteResult result) {
    this.result = result;
    this.resultLatch.countDown();
    this.affectRows = result.getAffectRows();
  }

  @Override
  public long getSpan() {
    return span;
  }

  @Override
  public void setSpan(long span) {
    this.span = span;
  }

  @Override
  public int getAffectedRows() {
    return affectRows;
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
