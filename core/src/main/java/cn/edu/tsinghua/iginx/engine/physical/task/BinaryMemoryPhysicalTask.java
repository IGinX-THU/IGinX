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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import javax.annotation.WillClose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BinaryMemoryPhysicalTask extends MemoryPhysicalTask {

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
  public TaskResult execute() {
    try (TaskResult leftResult = parentTaskA.getResult();
        TaskResult rightResult = parentTaskB.getResult()) {
      if (leftResult.isSuccessful() && rightResult.isSuccessful()) {
        BatchStream left = leftResult.unwrap();
        BatchStream right = rightResult.unwrap();
        BatchStream result = compute(left, right);
        return new TaskResult(result);
      }
    } catch (PhysicalException e) {
      return new TaskResult(e);
    }
    throw new IllegalStateException("unreachable code");
  }

  protected abstract BatchStream compute(@WillClose BatchStream left, @WillClose BatchStream right)
      throws PhysicalException;

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
