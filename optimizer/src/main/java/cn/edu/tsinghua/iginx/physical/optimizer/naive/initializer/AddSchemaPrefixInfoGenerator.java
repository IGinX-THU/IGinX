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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import org.apache.arrow.util.Preconditions;

public class AddSchemaPrefixInfoGenerator implements UnaryExecutorFactory<ProjectExecutor> {

  private final AddSchemaPrefix operator;

  public AddSchemaPrefixInfoGenerator(AddSchemaPrefix operator) {
    this.operator = Objects.requireNonNull(operator);
    Preconditions.checkNotNull(operator.getSchemaPrefix());
  }

  @Override
  public ProjectExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<ScalarExpression<?>> expressions = getExpression(context, inputSchema);
    return new ProjectExecutor(context, inputSchema.raw(), expressions);
  }

  public List<ScalarExpression<?>> getExpression(ExecutorContext context, BatchSchema inputSchema) {
    List<Pair<String, Integer>> columnsAndIndices = getColumnsAndIndices(inputSchema, operator);
    List<ScalarExpression<?>> ret = new ArrayList<>();
    for (Pair<String, Integer> pair : columnsAndIndices) {
      ret.add(new FieldNode(pair.v, pair.k));
    }
    return ret;
  }

  /**
   * 根据输入的BatchSchema计算AddSchemaPrefix算子的输出的列名和对应输入列的索引
   *
   * @param inputSchema 输入BatchSchema
   * @param addSchemaPrefix AddSchemaPrefix算子
   * @return 输出的列名和对应原来输入列的索引
   */
  protected static List<Pair<String, Integer>> getColumnsAndIndices(
      BatchSchema inputSchema, AddSchemaPrefix addSchemaPrefix) {
    List<Pair<String, Integer>> ret = new ArrayList<>();
    int start = inputSchema.hasKey() ? 1 : 0;
    int totalFieldSize = inputSchema.raw().getFields().size();
    if (inputSchema.hasKey()) {
      ret.add(new Pair<>(BatchSchema.KEY.getName(), 0));
    }
    String prefix = addSchemaPrefix.getSchemaPrefix();
    for (int i = start; i < totalFieldSize; i++) {
      ret.add(new Pair<>(prefix + "." + inputSchema.getName(i), i));
    }
    return ret;
  }
}
