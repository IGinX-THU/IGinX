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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.CrossJoinArrayList;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.CollectionJoinExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import java.util.*;

public class CrossJoinInfoGenerator implements BinaryExecutorFactory<StatefulBinaryExecutor> {

  private final CrossJoin operator;

  public CrossJoinInfoGenerator(CrossJoin operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  public StatefulBinaryExecutor initialize(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema)
      throws ComputeException {

    String leftPrefix = operator.getPrefixA();
    String rightPrefix = operator.getPrefixB();

    List<ScalarExpression<?>> outputExpressions = new ArrayList<>();
    for (int i = 0; i < leftSchema.getFieldCount(); i++) {
      if (i == leftSchema.getKeyIndex()) {
        if (leftPrefix != null) {
          outputExpressions.add(new FieldNode(i, leftPrefix + "." + leftSchema.getName(i)));
        }
        continue;
      }
      outputExpressions.add(new FieldNode(i));
    }

    for (int i = 0; i < rightSchema.getFieldCount(); i++) {
      if (i == rightSchema.getKeyIndex()) {
        if (rightPrefix != null) {
          outputExpressions.add(
              new FieldNode(
                  i + leftSchema.getFieldCount(), rightPrefix + "." + rightSchema.getName(i)));
        }
        continue;
      }
      outputExpressions.add(new FieldNode(i + leftSchema.getFieldCount()));
    }

    return new CollectionJoinExecutor(
        context,
        leftSchema,
        rightSchema,
        new CrossJoinArrayList.Builder(
            context.getAllocator(), leftSchema.raw(), rightSchema.raw(), outputExpressions),
        "CrossJoin");
  }
}
