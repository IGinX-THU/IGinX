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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive.NaiveOperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.StreamOperatorMemoryExecutor;

public class OperatorMemoryExecutorFactory {

  private static final OperatorMemoryExecutorFactory INSTANCE = new OperatorMemoryExecutorFactory();

  private OperatorMemoryExecutorFactory() {}

  public OperatorMemoryExecutor getMemoryExecutor() {
    if (ConfigDescriptor.getInstance().getConfig().isUseStreamExecutor()) {
      return StreamOperatorMemoryExecutor.getInstance();
    }
    return NaiveOperatorMemoryExecutor.getInstance();
  }

  public static OperatorMemoryExecutorFactory getInstance() {
    return INSTANCE;
  }
}
