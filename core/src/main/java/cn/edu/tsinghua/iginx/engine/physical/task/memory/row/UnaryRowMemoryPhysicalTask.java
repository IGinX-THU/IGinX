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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.row;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive.NaiveOperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import java.util.List;
import java.util.Objects;

public class UnaryRowMemoryPhysicalTask extends UnaryMemoryPhysicalTask<RowStream, RowStream> {

  private final UnaryOperator operator;

  public UnaryRowMemoryPhysicalTask(
      PhysicalTask<RowStream> parentTask,
      List<Operator> operators,
      UnaryOperator operator,
      RequestContext context) {
    super(parentTask, operators, context);
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  protected RowStream compute(RowStream previous) throws PhysicalException {
    Table input = NaiveOperatorMemoryExecutor.transformToTable(previous);
    try (StopWatch ignored = new StopWatch(getMetrics()::accumulateCpuTime)) {
      RowStream result =
          NaiveOperatorMemoryExecutor.getInstance()
              .executeUnaryOperator(operator, input, getContext());
      if (result instanceof Table) {
        getMetrics().accumulateAffectRows(((Table) result).getRows().size());
      }
      return result;
    }
  }

  @Override
  public Class<RowStream> getResultClass() {
    return RowStream.class;
  }
}
