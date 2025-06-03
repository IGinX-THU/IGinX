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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.row;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive.NaiveOperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import java.util.Collections;

public class BinaryRowMemoryPhysicalTask extends BinaryMemoryPhysicalTask<RowStream, RowStream> {

  private final BinaryOperator operator;

  public BinaryRowMemoryPhysicalTask(
      PhysicalTask<RowStream> parentTaskA,
      PhysicalTask<RowStream> parentTaskB,
      BinaryOperator operator,
      RequestContext context) {
    super(Collections.singletonList(operator), parentTaskA, parentTaskB, context);
    this.operator = operator;
  }

  @Override
  public Class<RowStream> getResultClass() {
    return RowStream.class;
  }

  @Override
  protected RowStream compute(RowStream left, RowStream right) throws PhysicalException {
    Table leftTable = NaiveOperatorMemoryExecutor.transformToTable(left);
    Table rightTable = NaiveOperatorMemoryExecutor.transformToTable(right);
    try (StopWatch ignored = new StopWatch(getMetrics()::accumulateCpuTime)) {
      RowStream result =
          NaiveOperatorMemoryExecutor.getInstance()
              .executeBinaryOperator(operator, leftTable, rightTable, getContext());
      if (result instanceof Table) {
        getMetrics().accumulateAffectRows(((Table) result).getRows().size());
      }
      return result;
    }
  }
}
