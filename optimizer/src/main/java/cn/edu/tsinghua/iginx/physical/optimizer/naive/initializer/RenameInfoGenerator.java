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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast.CastAsBigInt;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import org.apache.arrow.vector.types.pojo.Field;

public class RenameInfoGenerator implements UnaryExecutorFactory<ProjectExecutor> {

  private final Rename operator;

  public RenameInfoGenerator(Rename operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  public ProjectExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<Pair<String, Integer>> columnsAndIndices = getColumnsAndIndices(inputSchema, operator);
    int newKeyIndex = -1;
    for (Pair<String, Integer> pair : columnsAndIndices) {
      if (pair.k.equals(BatchSchema.KEY.getName())) {
        if (newKeyIndex != -1) {
          throw new ComputeException("Duplicate key column");
        }
        newKeyIndex = pair.v;
      }
    }
    List<ScalarExpression<?>> ret = new ArrayList<>();
    if (newKeyIndex != -1) {
      ret.add(
          new CallNode<>(
              new CastAsBigInt(false), BatchSchema.KEY.getName(), new FieldNode(newKeyIndex)));
    } else if (inputSchema.hasKey()) {
      ret.add(new FieldNode(inputSchema.getKeyIndex()));
    }
    for (Pair<String, Integer> pair : columnsAndIndices) {
      if (pair.v == newKeyIndex) {
        continue;
      }
      ret.add(new FieldNode(pair.v, pair.k));
    }
    return new ProjectExecutor(context, inputSchema.raw(), ret);
  }

  /**
   * 根据输入的BatchSchema计算Rename算子的输出的列名和对应输入列的索引
   *
   * @param inputSchema 输入BatchSchema
   * @param rename Rename算子
   * @return 输出的列名和对应原来输入列的索引
   */
  protected static List<Pair<String, Integer>> getColumnsAndIndices(
      BatchSchema inputSchema, Rename rename) {
    List<Pair<String, Integer>> ret = new ArrayList<>();
    int totalFieldSize = inputSchema.raw().getFields().size();
    int start = inputSchema.hasKey() ? 1 : 0;

    List<Pair<String, String>> aliasList = rename.getAliasList();
    List<String> ignorePatterns = rename.getIgnorePatterns();
    for (int i = start; i < totalFieldSize; i++) {
      Field field = inputSchema.getField(i);
      String name = field.getName();
      // 如果列名在ignorePatterns中，对该列不执行rename
      if (ignorePatterns.stream().anyMatch(pattern -> StringUtils.match(name, pattern))) {
        ret.add(new Pair<>(name, i));
        continue;
      }

      String alias = "";
      for (Pair<String, String> pair : aliasList) {
        String oldPattern = pair.k;
        String newPattern = pair.v;
        if (oldPattern.equals("*") && newPattern.endsWith(".*")) {
          String newPrefix = newPattern.substring(0, newPattern.length() - 1);
          alias = newPrefix + name;
        } else if (oldPattern.endsWith(".*") && newPattern.endsWith(".*")) {
          String oldPrefix = oldPattern.substring(0, oldPattern.length() - 1);
          String newPrefix = newPattern.substring(0, newPattern.length() - 1);
          if (name.startsWith(oldPrefix)) {
            alias = name.replaceFirst(oldPrefix, newPrefix);
          }
          break;
        } else if (oldPattern.equals(name)) {
          alias = newPattern;
          Set<Map<String, String>> tagSet = new HashSet<>();
          Field nextField = i < totalFieldSize - 1 ? inputSchema.getField(i + 1) : null;
          tagSet.add(field.getMetadata());
          // 处理同一列但不同tag的情况
          while (nextField != null
              && oldPattern.equals(nextField.getName())
              && !tagSet.contains(nextField.getMetadata())) {
            // newFields.add(new Field(alias, field.getType(), field.getMetadata()));
            ret.add(new Pair<>(alias, i));
            field = nextField;
            i++;
            nextField = i < totalFieldSize - 1 ? inputSchema.getField(i + 1) : null;
            tagSet.add(field.getMetadata());
          }
          aliasList.remove(pair);
          break;
        } else {
          if (StringUtils.match(name, oldPattern)) {
            if (newPattern.endsWith("." + oldPattern)) {
              String prefix = newPattern.substring(0, newPattern.length() - oldPattern.length());
              alias = prefix + name;
            } else {
              alias = newPattern;
            }
            break;
          }
        }
      }
      if (alias.isEmpty()) {
        ret.add(new Pair<>(name, i));
      } else {
        ret.add(new Pair<>(alias, i));
      }
    }
    return ret;
  }
}
