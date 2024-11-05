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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.HashJoinExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.util.HashJoins;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MarkJoinInfoGenerator implements BinaryExecutorFactory<StatefulBinaryExecutor> {

  private final MarkJoin operator;

  public MarkJoinInfoGenerator(MarkJoin operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  public StatefulBinaryExecutor initialize(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema)
      throws ComputeException {
    switch (operator.getJoinAlgType()) {
      case HashJoin:
        return initializeHashJoin(context, leftSchema, rightSchema);
      default:
        throw new IllegalStateException(
            "JoinAlgType is not supported: " + operator.getJoinAlgType());
    }
  }

  public HashJoinExecutor initializeHashJoin(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema)
      throws ComputeException {
    JoinOption joinOption = JoinOption.ofMark(operator.getMarkColumn(), operator.isAntiJoin());

    List<Filter> subFilters = new ArrayList<>();
    if (!operator.getFilter().equals(new BoolFilter(true))) {
      subFilters.add(operator.getFilter());
    }
    for (String extraPrefix : operator.getExtraJoinPrefix()) {
      subFilters.add(new PathFilter(extraPrefix, Op.E, extraPrefix));
    }

    return HashJoins.constructHashJoin(
        context, leftSchema, rightSchema, new AndFilter(subFilters), joinOption);
  }
}
