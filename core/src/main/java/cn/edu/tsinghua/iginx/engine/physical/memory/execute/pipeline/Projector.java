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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.Field;

public class Projector extends PipelineExecutor {

  private final UnaryOperator operator;

  private List<Pair<String, Integer>> columnsAndIndices; // 输出的列名和对应输入列的索引

  private BatchSchema outputSchema;

  public Projector(final UnaryOperator operator) {
    this.operator = operator;
  }

  @Override
  public String getDescription() {
    return "Projector(" + operator.getType() + "): [" + operator.getInfo() + "]";
  }

  @Override
  public void close() throws PhysicalException {}

  @Override
  protected BatchSchema internalInitialize(BatchSchema inputSchema) throws PhysicalException {
    if (outputSchema != null) {
      return outputSchema;
    }
    switch (operator.getType()) {
      case Project:
        columnsAndIndices = getColumnsAndIndices(inputSchema, (Project) operator);
        break;
      case Reorder:
        columnsAndIndices = getColumnsAndIndices(inputSchema, (Reorder) operator);
        break;
      case Rename:
        columnsAndIndices = getColumnsAndIndices(inputSchema, (Rename) operator);
        break;
      case AddSchemaPrefix:
        columnsAndIndices = getColumnsAndIndices(inputSchema, (AddSchemaPrefix) operator);
        break;
      default:
        throw new UnexpectedOperatorException(
            "Unexpected operator type in Projector: " + operator.getType());
    }

    // 生成输出结果的BatchSchema
    BatchSchema.Builder builder = BatchSchema.builder();
    if (inputSchema.hasKey()) {
      builder.withKey();
    }
    int start = inputSchema.hasKey() ? 1 : 0;
    for (int i = start; i < columnsAndIndices.size(); i++) {
      Pair<String, Integer> pair = columnsAndIndices.get(i);
      builder.addField(pair.k, inputSchema.getFieldArrowType(pair.v), inputSchema.getTag(pair.v));
    }
    outputSchema = builder.build();
    return outputSchema;
  }

  @Override
  protected Batch internalCompute(Batch batch) throws PhysicalException {
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (Pair<String, Integer> pair : columnsAndIndices) {
      fieldVectors.add(batch.raw().getVectorCopy(pair.v)); // TODO:能否不复制或少复制数据
    }
    return new Batch(new Table(fieldVectors), outputSchema);
  }

  /**
   * 根据输入的BatchSchema计算Project算子的输出的列名和对应输入列的索引
   *
   * @param inputSchema 输入BatchSchema
   * @param project Project算子
   * @return 输出的列名和对应输入列的索引
   */
  public static List<Pair<String, Integer>> getColumnsAndIndices(
      BatchSchema inputSchema, Project project) {
    List<Pair<String, Integer>> ret = new ArrayList<>();
    int start = inputSchema.hasKey() ? 1 : 0;
    int totalFieldSize = inputSchema.raw().getFields().size();
    if (inputSchema.hasKey()) {
      ret.add(new Pair<>(Constants.KEY, 0));
    }
    List<String> patterns = project.getPatterns();
    for (int i = start; i < totalFieldSize; i++) {
      String name = inputSchema.getFieldName(i);
      if (project.isRemainKey() && name.endsWith(Constants.KEY)) {
        ret.add(new Pair<>(name, i));
        continue;
      }
      for (String pattern : patterns) {
        if (!StringUtils.isPattern(pattern)) {
          if (pattern.equals(name)) {
            ret.add(new Pair<>(name, i));
          }
        } else {
          if (Pattern.matches(StringUtils.reformatPath(pattern), name)) {
            ret.add(new Pair<>(name, i));
          }
        }
      }
    }
    return ret;
  }

  /**
   * 根据输入的BatchSchema计算Reorder算子的输出的列名和对应输入列的索引
   *
   * @param inputSchema 输入BatchSchema
   * @param reorder Reorder算子
   * @return 输出的列名和对应原来输入列的索引
   */
  public static List<Pair<String, Integer>> getColumnsAndIndices(
      BatchSchema inputSchema, Reorder reorder) {
    List<Pair<String, Integer>> ret = new ArrayList<>();
    int start = inputSchema.hasKey() ? 1 : 0;
    int totalFieldSize = inputSchema.raw().getFields().size();
    if (inputSchema.hasKey()) {
      ret.add(new Pair<>(Constants.KEY, 0));
    }
    List<String> patterns = reorder.getPatterns();
    List<Boolean> isPyUDFList = reorder.getIsPyUDF();

    // 保留关键字列
    for (int i = start; i < totalFieldSize; i++) {
      String name = inputSchema.getFieldName(i);
      if (Constants.RESERVED_COLS.contains(name)) {
        ret.add(new Pair<>(name, i));
      }
    }

    int size = patterns.size();
    for (int index = 0; index < size; index++) {
      String pattern = patterns.get(index);
      List<Pair<String, Integer>> matchedFields = new ArrayList<>();
      if (StringUtils.isPattern(pattern)) {
        for (int i = start; i < totalFieldSize; i++) {
          String name = inputSchema.getFieldName(i);
          if (StringUtils.match(name, pattern)) {
            matchedFields.add(new Pair<>(name, i));
          }
        }
      } else {
        for (int i = start; i < totalFieldSize; i++) {
          String name = inputSchema.getFieldName(i);
          if (pattern.equals(name)) {
            matchedFields.add(new Pair<>(name, i));
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
    return ret;
  }

  /**
   * 根据输入的BatchSchema计算Rename算子的输出的列名和对应输入列的索引
   *
   * @param inputSchema 输入BatchSchema
   * @param rename Rename算子
   * @return 输出的列名和对应原来输入列的索引
   */
  public static List<Pair<String, Integer>> getColumnsAndIndices(
      BatchSchema inputSchema, Rename rename) {
    List<Pair<String, Integer>> ret = new ArrayList<>();
    int start = inputSchema.hasKey() ? 1 : 0;
    int totalFieldSize = inputSchema.raw().getFields().size();
    if (inputSchema.hasKey()) {
      ret.add(new Pair<>(Constants.KEY, 0));
    }

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

  /**
   * 根据输入的BatchSchema计算AddSchemaPrefix算子的输出的列名和对应输入列的索引
   *
   * @param inputSchema 输入BatchSchema
   * @param addSchemaPrefix AddSchemaPrefix算子
   * @return 输出的列名和对应原来输入列的索引
   */
  public static List<Pair<String, Integer>> getColumnsAndIndices(
      BatchSchema inputSchema, AddSchemaPrefix addSchemaPrefix) {
    List<Pair<String, Integer>> ret = new ArrayList<>();
    int start = inputSchema.hasKey() ? 1 : 0;
    int totalFieldSize = inputSchema.raw().getFields().size();
    if (inputSchema.hasKey()) {
      ret.add(new Pair<>(Constants.KEY, 0));
    }
    String prefix = addSchemaPrefix.getSchemaPrefix();
    for (int i = start; i < totalFieldSize; i++) {
      ret.add(new Pair<>(prefix + "." + inputSchema.getFieldName(i), i));
    }
    return ret;
  }
}
