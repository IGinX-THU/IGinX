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
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class ProjectInfoGenerator implements UnaryExecutorFactory<ProjectExecutor> {

  private final Project operator;

  public ProjectInfoGenerator(Project operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  public ProjectExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<Pair<String, Integer>> columnsAndIndices = getColumnsAndIndices(inputSchema, operator);
    List<ScalarExpression<?>> ret = new ArrayList<>();
    for (Pair<String, Integer> pair : columnsAndIndices) {
      ret.add(new FieldNode(pair.v, pair.k));
    }
    return new ProjectExecutor(context, inputSchema.raw(), ret);
  }

  /**
   * 根据输入的BatchSchema计算Project算子的输出的列名和对应输入列的索引
   *
   * @param inputSchema 输入BatchSchema
   * @param project Project算子
   * @return 输出的列名和对应输入列的索引
   */
  protected static List<Pair<String, Integer>> getColumnsAndIndices(
      BatchSchema inputSchema, Project project) {
    List<Pair<String, Integer>> ret = new ArrayList<>();
    int start = inputSchema.hasKey() ? 1 : 0;
    int totalFieldSize = inputSchema.raw().getFields().size();
    if (inputSchema.hasKey()) {
      ret.add(new Pair<>(BatchSchema.KEY.getName(), 0));
    }
    List<String> patterns = project.getPatterns();
    for (int i = start; i < totalFieldSize; i++) {
      String name = inputSchema.getName(i);
      if (project.isRemainKey() && name.endsWith(BatchSchema.KEY.getName())) {
        ret.add(new Pair<>(name, i));
        continue;
      }
      for (String pattern : patterns) {
        if (!StringUtils.isPattern(pattern)) {
          if (pattern.equals(name)) {
            ret.add(new Pair<>(name, i));
            break;
          }
        } else {
          if (Pattern.matches(StringUtils.reformatPath(pattern), name)) {
            ret.add(new Pair<>(name, i));
            break;
          }
        }
      }
    }
    return ret;
  }
}
