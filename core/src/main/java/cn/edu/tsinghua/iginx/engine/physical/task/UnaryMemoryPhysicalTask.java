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
import java.util.Collections;
import javax.annotation.WillClose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UnaryMemoryPhysicalTask extends MemoryPhysicalTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnaryMemoryPhysicalTask.class);

  private PhysicalTask parentTask;

  public UnaryMemoryPhysicalTask(PhysicalTask parentTask, RequestContext context) {
    super(TaskType.UnaryMemory, Collections.emptyList(), context);
    this.parentTask = parentTask;
  }

  public void setParentTask(PhysicalTask parentTask) {
    this.parentTask = parentTask;
  }

  public PhysicalTask getParentTask() {
    return parentTask;
  }

  @Override
  public TaskResult execute() {
    try (TaskResult parentResult = parentTask.getResult()) {
      BatchStream stream = parentResult.unwrap();
      BatchStream result = compute(stream);
      return new TaskResult(result);
    } catch (PhysicalException e) {
      return new TaskResult(e);
    }
  }

  protected abstract BatchStream compute(@WillClose BatchStream previous) throws PhysicalException;

  @Override
  public boolean notifyParentReady() {
    return parentReadyCount.incrementAndGet() == 1;
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    PhysicalTask task = getParentTask();
    if (task != null) {
      task.accept(visitor);
    }
    visitor.leave();
  }

  @Override
  public abstract String getInfo();
}
