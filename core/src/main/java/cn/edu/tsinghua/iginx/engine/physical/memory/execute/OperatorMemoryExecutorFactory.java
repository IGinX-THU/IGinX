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
