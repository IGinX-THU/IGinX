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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.sort.IndexSortExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.CompareOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.InnerBatchSortUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.arrow.util.Preconditions;

public class InnerBatchSortInfoGenerator
    implements UnaryExecutorFactory<InnerBatchSortUnaryExecutor> {

  private final Sort sort;

  public InnerBatchSortInfoGenerator(Sort sort) {
    this.sort = Objects.requireNonNull(sort);
  }

  @Override
  public InnerBatchSortUnaryExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    IndexSortExpression indexSortExpression = createIndexSortExpression(sort, inputSchema);
    return new InnerBatchSortUnaryExecutor(context, inputSchema.raw(), indexSortExpression);
  }

  private static IndexSortExpression createIndexSortExpression(Sort sort, BatchSchema inputSchema)
      throws ComputeException {
    Preconditions.checkArgument(sort.getSortByCols().size() == sort.getSortTypes().size());

    List<ScalarExpression<?>> children = new ArrayList<>();
    for (int i = 0; i < sort.getSortByCols().size(); i++) {
      String col = sort.getSortByCols().get(i);
      List<Integer> partialIndices = Schemas.matchPatternIgnoreKey(inputSchema.raw(), col);
      if (partialIndices.isEmpty()) {
        throw new ComputeException(
            "SortBy Column " + col + " not found in input schema " + inputSchema);
      } else if (partialIndices.size() > 1) {
        throw new ComputeException(
            "SortBy Column " + col + " is ambiguous in input schema " + inputSchema);
      }
      int index = partialIndices.get(0);
      children.add(new FieldNode(index));
    }

    List<CompareOption> options =
        sort.getSortTypes().stream()
            .map(InnerBatchSortInfoGenerator::createCompareOption)
            .collect(Collectors.toList());
    return new IndexSortExpression(options, children);
  }

  private static CompareOption createCompareOption(Sort.SortType sort) {
    switch (sort) {
      case ASC:
        return CompareOption.ASC;
      case DESC:
        return CompareOption.DESC;
      default:
        throw new IllegalArgumentException("Unknown sort type " + sort);
    }
  }
}
