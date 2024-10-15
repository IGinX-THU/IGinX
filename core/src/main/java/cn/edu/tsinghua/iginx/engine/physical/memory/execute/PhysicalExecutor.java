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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import java.util.Objects;

public abstract class PhysicalExecutor implements AutoCloseable {

  private ExecutorContext context = null;

  protected void initialize(ExecutorContext context) {
    if (this.context != null) {
      throw new IllegalStateException("Already initialized");
    }
    this.context = Objects.requireNonNull(context);
  }

  protected ExecutorContext getContext() {
    if (context == null) {
      throw new IllegalStateException("Not initialized");
    }
    return context;
  }

  public abstract String getDescription();

  @Override
  public abstract void close() throws ComputeException;

  @Override
  public String toString() {
    return getDescription();
  }
}
