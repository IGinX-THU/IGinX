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
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;

public class ReorderInfoGenerator implements UnaryExecutorFactory<ProjectExecutor> {

  private final Reorder operator;

  public ReorderInfoGenerator(Reorder operator) {
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
    boolean empty = ret.isEmpty() || (inputSchema.hasKey() && ret.size() == 1);
    return new ProjectExecutor(context, inputSchema.raw(), ret, empty);
  }

  /**
   * 根据输入的BatchSchema计算Reorder算子的输出的列名和对应输入列的索引
   *
   * @param inputSchema 输入BatchSchema
   * @param reorder Reorder算子
   * @return 输出的列名和对应原来输入列的索引
   */
  protected static List<Pair<String, Integer>> getColumnsAndIndices(
      BatchSchema inputSchema, Reorder reorder) throws ComputeException {
    List<Pair<String, Integer>> ret = new ArrayList<>();
    int start = inputSchema.hasKey() ? 1 : 0;
    int totalFieldSize = inputSchema.raw().getFields().size();
    if (inputSchema.hasKey()) {
      ret.add(new Pair<>(BatchSchema.KEY.getName(), 0));
    }
    List<String> patterns = reorder.getPatterns();
    List<Boolean> isPyUDFList = reorder.getIsPyUDF();

    // 保留关键字列
    for (int i = start; i < totalFieldSize; i++) {
      String name = inputSchema.getName(i);
      if (Constants.RESERVED_COLS.contains(name)) {
        ret.add(
            new Pair<>(
                TagKVUtils.toFullName(
                    inputSchema.getName(i), inputSchema.getField(i).getMetadata()),
                i));
      }
    }

    int size = patterns.size();
    for (int index = 0; index < size; index++) {
      String pattern = patterns.get(index);
      List<Pair<String, Integer>> matchedFields = new ArrayList<>();
      if (StringUtils.isPattern(pattern)) {
        for (int i = start; i < totalFieldSize; i++) {
          String name = inputSchema.getName(i);
          if (StringUtils.match(name, pattern)) {
            matchedFields.add(
                new Pair<>(
                    TagKVUtils.toFullName(
                        inputSchema.getName(i), inputSchema.getField(i).getMetadata()),
                    i));
          }
        }
      } else {
        for (int i = start; i < totalFieldSize; i++) {
          String name = inputSchema.getName(i);
          if (pattern.equals(name)) {
            matchedFields.add(
                new Pair<>(
                    TagKVUtils.toFullName(
                        inputSchema.getName(i), inputSchema.getField(i).getMetadata()),
                    i));
          }
        }
      }
      if (!matchedFields.isEmpty()) {
        // 不对同一个UDF里返回的多列进行重新排序
        if (!isPyUDFList.get(index)) {
          matchedFields.sort(Comparator.comparing(Pair::getK));
        }
        ret.addAll(matchedFields);
      }
    }
    return ret.stream()
        .map(Pair::getV)
        .map(
            index -> {
              Field field = inputSchema.getField(index);
              return new Pair<>(field.getName(), index);
            })
        .collect(Collectors.toList());
  }
}
