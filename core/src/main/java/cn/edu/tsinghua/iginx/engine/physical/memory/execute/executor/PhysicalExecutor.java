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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseable;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

/** 该类是物理执行器的抽象类，所有的物理执行器都应该继承该类。 引入物理执行器的目的是为了将计算任务的执行与计算任务的调度解耦。 */
public abstract class PhysicalExecutor implements ComputingCloseable {

  protected final ExecutorContext context;

  protected PhysicalExecutor(ExecutorContext context) {
    this.context = Objects.requireNonNull(context);
  }

  @Override
  public String toString() {
    return getInfo();
  }

  public abstract Schema getOutputSchema() throws ComputeException;

  protected abstract String getInfo();
}
