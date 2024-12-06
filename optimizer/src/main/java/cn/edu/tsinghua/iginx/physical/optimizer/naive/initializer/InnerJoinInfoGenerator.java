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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import static cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType.HashJoin;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.util.HashJoinUtils;
import java.util.*;

public class InnerJoinInfoGenerator implements BinaryExecutorFactory<StatefulBinaryExecutor> {

  private final InnerJoin operator;

  public InnerJoinInfoGenerator(InnerJoin operator) {
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

  private StatefulBinaryExecutor initializeHashJoin(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema)
      throws ComputeException {

    if (operator.getJoinAlgType() != HashJoin) {
      throw new IllegalArgumentException(
          "JoinAlgType is not HashJoin: " + operator.getJoinAlgType());
    }

    List<Filter> subFilters = new ArrayList<>();
    if (operator.getFilter() != null) {
      subFilters.add(operator.getFilter());
    }
    for (String extraPrefix : operator.getExtraJoinPrefix()) {
      subFilters.add(new PathFilter(extraPrefix, Op.E, extraPrefix));
    }
    Set<String> ignoreColumns = new HashSet<>();
    for (String joinColumn : operator.getJoinColumns()) {
      ignoreColumns.add(operator.getPrefixA() + "." + joinColumn);
      subFilters.add(
          new PathFilter(
              operator.getPrefixA() + "." + joinColumn,
              Op.E,
              operator.getPrefixB() + "." + joinColumn));
    }

    return HashJoinUtils.constructHashJoin(
        context,
        leftSchema,
        rightSchema,
        operator.getPrefixA(),
        operator.getPrefixB(),
        new AndFilter(subFilters),
        ignoreColumns,
        JoinOption.INNER);
  }
}
