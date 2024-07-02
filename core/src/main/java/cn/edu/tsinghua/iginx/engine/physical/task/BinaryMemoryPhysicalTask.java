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
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryMemoryPhysicalTask extends MemoryPhysicalTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(BinaryMemoryPhysicalTask.class);

  private final PhysicalTask parentTaskA;

  private final PhysicalTask parentTaskB;

  public BinaryMemoryPhysicalTask(
      List<Operator> operators,
      PhysicalTask parentTaskA,
      PhysicalTask parentTaskB,
      RequestContext context) {
    super(TaskType.BinaryMemory, operators, context);
    this.parentTaskA = parentTaskA;
    this.parentTaskB = parentTaskB;
  }

  public PhysicalTask getParentTaskA() {
    return parentTaskA;
  }

  public PhysicalTask getParentTaskB() {
    return parentTaskB;
  }

  @Override
  public TaskExecuteResult execute() {
    TaskExecuteResult parentResultA = parentTaskA.getResult();
    if (parentResultA == null) {
      return new TaskExecuteResult(
          new PhysicalException("unexpected parent task execute result for " + this + ": null"));
    }
    if (parentResultA.getException() != null) {
      return parentResultA;
    }
    TaskExecuteResult parentResultB = parentTaskB.getResult();
    if (parentResultB == null) {
      return new TaskExecuteResult(
          new PhysicalException("unexpected parent task execute result for " + this + ": null"));
    }
    if (parentResultB.getException() != null) {
      return parentResultB;
    }
    List<Operator> operators = getOperators();
    RowStream streamA = parentResultA.getRowStream();
    RowStream streamB = parentResultB.getRowStream();
    RowStream stream;
    OperatorMemoryExecutor executor =
        OperatorMemoryExecutorFactory.getInstance().getMemoryExecutor();
    try {
      Operator op = operators.get(0);
      if (!OperatorType.isBinaryOperator(op.getType())) {
        throw new UnexpectedOperatorException("unexpected operator " + op + " in binary task");
      }
      stream = executor.executeBinaryOperator((BinaryOperator) op, streamA, streamB, getContext());
      for (int i = 1; i < operators.size(); i++) {
        op = operators.get(i);
        if (OperatorType.isBinaryOperator(op.getType())) {
          throw new UnexpectedOperatorException(
              "unexpected binary operator " + op + " in unary task");
        }
        stream = executor.executeUnaryOperator((UnaryOperator) op, stream, getContext());
      }
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when execute operator in memory: ", e);
      return new TaskExecuteResult(e);
    }
    return new TaskExecuteResult(stream);
  }

  @Override
  public boolean notifyParentReady() {
    return parentReadyCount.incrementAndGet() == 2;
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    PhysicalTask taskA = getParentTaskA();
    if (taskA != null) {
      taskA.accept(visitor);
    }
    PhysicalTask taskB = getParentTaskB();
    if (taskB != null) {
      taskB.accept(visitor);
    }
    visitor.leave();
  }
}
