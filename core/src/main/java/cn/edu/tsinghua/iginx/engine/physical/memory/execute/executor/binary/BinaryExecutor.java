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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.PhysicalExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.Objects;

public abstract class BinaryExecutor extends PhysicalExecutor {

  protected final BatchSchema leftSchema;
  protected final BatchSchema rightSchema;

  protected BinaryExecutor(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema) {
    super(context);
    this.leftSchema = Objects.requireNonNull(leftSchema);
    this.rightSchema = Objects.requireNonNull(rightSchema);
  }

  public BatchSchema getLeftSchema() {
    return leftSchema;
  }

  public BatchSchema getRightSchema() {
    return rightSchema;
  }
}
