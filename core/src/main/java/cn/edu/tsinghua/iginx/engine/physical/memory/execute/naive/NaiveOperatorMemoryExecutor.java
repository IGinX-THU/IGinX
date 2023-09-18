/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils.calculateHashJoinPath;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils.checkHeadersComparable;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils.constructNewHead;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.checkJoinColumns;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.checkNeedTypeCast;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.combineMultipleColumns;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.equalOnSpecificPaths;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.establishHashMap;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.getSamePathWithSpecificPrefix;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.isValueEqualRow;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.removeDuplicateRows;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.KEY;
import static cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils.isCanUseSetQuantifierFunction;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils.getHash;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Min;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Downsample;
import cn.edu.tsinghua.iginx.engine.shared.operator.Except;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Intersect;
import cn.edu.tsinghua.iginx.engine.shared.operator.Join;
import cn.edu.tsinghua.iginx.engine.shared.operator.Limit;
import cn.edu.tsinghua.iginx.engine.shared.operator.MappingTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.PathUnion;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort.SortType;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Union;
import cn.edu.tsinghua.iginx.engine.shared.operator.ValueToSelectedPath;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NaiveOperatorMemoryExecutor implements OperatorMemoryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(NaiveOperatorMemoryExecutor.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private NaiveOperatorMemoryExecutor() {}

  public static NaiveOperatorMemoryExecutor getInstance() {
    return NaiveOperatorMemoryExecutorHolder.INSTANCE;
  }

  @Override
  public RowStream executeUnaryOperator(UnaryOperator operator, RowStream stream)
      throws PhysicalException {
    switch (operator.getType()) {
      case Project:
        return executeProject((Project) operator, transformToTable(stream));
      case Select:
        return executeSelect((Select) operator, transformToTable(stream));
      case Sort:
        return executeSort((Sort) operator, transformToTable(stream));
      case Limit:
        return executeLimit((Limit) operator, transformToTable(stream));
      case Downsample:
        return executeDownsample((Downsample) operator, transformToTable(stream));
      case RowTransform:
        return executeRowTransform((RowTransform) operator, transformToTable(stream));
      case SetTransform:
        return executeSetTransform((SetTransform) operator, transformToTable(stream));
      case MappingTransform:
        return executeMappingTransform((MappingTransform) operator, transformToTable(stream));
      case Rename:
        return executeRename((Rename) operator, transformToTable(stream));
      case Reorder:
        return executeReorder((Reorder) operator, transformToTable(stream));
      case AddSchemaPrefix:
        return executeAddSchemaPrefix((AddSchemaPrefix) operator, transformToTable(stream));
      case GroupBy:
        return executeGroupBy((GroupBy) operator, transformToTable(stream));
      case Distinct:
        return executeDistinct(transformToTable(stream));
      case ValueToSelectedPath:
        return executeValueToSelectedPath((ValueToSelectedPath) operator, transformToTable(stream));
      default:
        throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
    }
  }

  @Override
  public RowStream executeBinaryOperator(
      BinaryOperator operator, RowStream streamA, RowStream streamB) throws PhysicalException {
    switch (operator.getType()) {
      case Join:
        return executeJoin((Join) operator, transformToTable(streamA), transformToTable(streamB));
      case CrossJoin:
        return executeCrossJoin(
            (CrossJoin) operator, transformToTable(streamA), transformToTable(streamB));
      case InnerJoin:
        return executeInnerJoin(
            (InnerJoin) operator, transformToTable(streamA), transformToTable(streamB));
      case OuterJoin:
        return executeOuterJoin(
            (OuterJoin) operator, transformToTable(streamA), transformToTable(streamB));
      case SingleJoin:
        return executeSingleJoin(
            (SingleJoin) operator, transformToTable(streamA), transformToTable(streamB));
      case MarkJoin:
        return executeMarkJoin(
            (MarkJoin) operator, transformToTable(streamA), transformToTable(streamB));
      case PathUnion:
        return executePathUnion(
            (PathUnion) operator, transformToTable(streamA), transformToTable(streamB));
      case Union:
        return executeUnion((Union) operator, transformToTable(streamA), transformToTable(streamB));
      case Except:
        return executeExcept(
            (Except) operator, transformToTable(streamA), transformToTable(streamB));
      case Intersect:
        return executeIntersect(
            (Intersect) operator, transformToTable(streamA), transformToTable(streamB));
      default:
        throw new UnexpectedOperatorException("unknown binary operator: " + operator.getType());
    }
  }

  private Table transformToTable(RowStream stream) throws PhysicalException {
    if (stream instanceof Table) {
      return (Table) stream;
    }
    Header header = stream.getHeader();
    List<Row> rows = new ArrayList<>();
    while (stream.hasNext()) {
      rows.add(stream.next());
    }
    stream.close();
    return new Table(header, rows);
  }

  private RowStream executeProject(Project project, Table table) throws PhysicalException {
    List<String> patterns = project.getPatterns();
    Header header = table.getHeader();
    List<Field> targetFields = new ArrayList<>();

    for (Field field : header.getFields()) {
      if (field.getName().endsWith(KEY)) {
        targetFields.add(field);
        continue;
      }
      for (String pattern : patterns) {
        if (!StringUtils.isPattern(pattern)) {
          if (pattern.equals(field.getName())) {
            targetFields.add(field);
          }
        } else {
          if (Pattern.matches(StringUtils.reformatPath(pattern), field.getName())) {
            targetFields.add(field);
          }
        }
      }
    }
    Header targetHeader = new Header(header.getKey(), targetFields);
    List<Row> targetRows = new ArrayList<>();
    while (table.hasNext()) {
      Row row = table.next();
      Object[] objects = new Object[targetFields.size()];
      for (int i = 0; i < targetFields.size(); i++) {
        objects[i] = row.getValue(targetFields.get(i));
      }
      if (header.hasKey()) {
        targetRows.add(new Row(targetHeader, row.getKey(), objects));
      } else {
        targetRows.add(new Row(targetHeader, objects));
      }
    }
    return new Table(targetHeader, targetRows);
  }

  private RowStream executeSelect(Select select, Table table) throws PhysicalException {
    Filter filter = select.getFilter();
    List<Row> rows = table.getRows();

    List<Row> targetRows = RowUtils.cacheFilterResult(rows, filter);
    return new Table(table.getHeader(), targetRows);
  }

  private RowStream executeSort(Sort sort, Table table) throws PhysicalException {
    RowUtils.sortRows(table.getRows(), sort.getSortType() == SortType.ASC, sort.getSortByCols());
    return table;
  }

  private RowStream executeLimit(Limit limit, Table table) throws PhysicalException {
    int rowSize = table.getRowSize();
    Header header = table.getHeader();
    List<Row> rows = new ArrayList<>();
    if (rowSize > limit.getOffset()) { // 没有把所有的行都跳过
      for (int i = limit.getOffset();
          i < rowSize && i - limit.getOffset() < limit.getLimit();
          i++) {
        rows.add(table.getRow(i));
      }
    }
    return new Table(header, rows);
  }

  private RowStream executeDownsample(Downsample downsample, Table table) throws PhysicalException {
    Header header = table.getHeader();
    if (!header.hasKey()) {
      throw new InvalidOperatorParameterException(
          "downsample operator is not support for row stream without timestamps.");
    }
    List<Row> rows = table.getRows();
    long bias = downsample.getKeyRange().getActualBeginKey();
    long endKey = downsample.getKeyRange().getActualEndKey();
    long precision = downsample.getPrecision();
    long slideDistance = downsample.getSlideDistance();
    // startKey + (n - 1) * slideDistance + precision - 1 >= endKey
    long n = (int) (Math.ceil((double) (endKey - bias - precision + 1) / slideDistance) + 1);
    TreeMap<Long, List<Row>> groups = new TreeMap<>();
    SetMappingFunction function = (SetMappingFunction) downsample.getFunctionCall().getFunction();
    FunctionParams params = downsample.getFunctionCall().getParams();
    if (precision == slideDistance) {
      for (Row row : rows) {
        long timestamp = row.getKey() - (row.getKey() - bias) % precision;
        groups.compute(timestamp, (k, v) -> v == null ? new ArrayList<>() : v).add(row);
      }
    } else {
      HashMap<Long, Long> timestamps = new HashMap<>();
      for (long i = 0; i < n; i++) {
        timestamps.put(i, bias + i * slideDistance);
      }
      for (Row row : rows) {
        long rowTimestamp = row.getKey();
        for (long i = 0; i < n; i++) {
          if (rowTimestamp - timestamps.get(i) >= 0
              && rowTimestamp - timestamps.get(i) < precision) {
            groups.compute(timestamps.get(i), (k, v) -> v == null ? new ArrayList<>() : v).add(row);
          }
        }
      }
    }
    List<Pair<Long, Row>> transformedRawRows = new ArrayList<>();
    try {
      for (Map.Entry<Long, List<Row>> entry : groups.entrySet()) {
        long time = entry.getKey();
        List<Row> group = entry.getValue();

        if (params.isDistinct()) {
          if (!isCanUseSetQuantifierFunction(function.getIdentifier())) {
            throw new IllegalArgumentException(
                "function " + function.getIdentifier() + " can't use DISTINCT");
          }
          // min和max无需去重
          if (!function.getIdentifier().equals(Max.MAX)
              && !function.getIdentifier().equals(Min.MIN)) {
            group = removeDuplicateRows(group);
          }
        }

        Row row = function.transform(new Table(header, group), params);
        if (row != null) {
          transformedRawRows.add(new Pair<>(time, row));
        }
      }
    } catch (Exception e) {
      throw new PhysicalTaskExecuteFailureException(
          "encounter error when execute set mapping function " + function.getIdentifier() + ".", e);
    }
    if (transformedRawRows.size() == 0) {
      return Table.EMPTY_TABLE;
    }
    Header newHeader = new Header(Field.KEY, transformedRawRows.get(0).v.getHeader().getFields());
    List<Row> transformedRows = new ArrayList<>();
    for (Pair<Long, Row> pair : transformedRawRows) {
      transformedRows.add(new Row(newHeader, pair.k, pair.v.getValues()));
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeRowTransform(RowTransform rowTransform, Table table)
      throws PhysicalException {
    List<Pair<RowMappingFunction, FunctionParams>> list = new ArrayList<>();
    rowTransform
        .getFunctionCallList()
        .forEach(
            functionCall -> {
              list.add(
                  new Pair<>(
                      (RowMappingFunction) functionCall.getFunction(), functionCall.getParams()));
            });

    List<Row> rows = new ArrayList<>();
    while (table.hasNext()) {
      Row current = table.next();
      List<Row> columnList = new ArrayList<>();
      list.forEach(
          pair -> {
            RowMappingFunction function = pair.k;
            FunctionParams params = pair.v;
            try {
              // 分别计算每个表达式得到相应的结果
              Row column = function.transform(current, params);
              if (column != null) {
                columnList.add(column);
              }
            } catch (Exception e) {
              try {
                throw new PhysicalTaskExecuteFailureException(
                    "encounter error when execute row mapping function "
                        + function.getIdentifier()
                        + ".",
                    e);
              } catch (PhysicalTaskExecuteFailureException ex) {
                throw new RuntimeException(ex);
              }
            }
          });
      // 如果计算结果都不为空，将计算结果合并成一行
      if (columnList.size() == list.size()) {
        rows.add(combineMultipleColumns(columnList));
      }
    }
    if (rows.size() == 0) {
      return Table.EMPTY_TABLE;
    }
    Header header = rows.get(0).getHeader();
    return new Table(header, rows);
  }

  private RowStream executeSetTransform(SetTransform setTransform, Table table)
      throws PhysicalException {
    SetMappingFunction function = (SetMappingFunction) setTransform.getFunctionCall().getFunction();
    FunctionParams params = setTransform.getFunctionCall().getParams();

    if (params.isDistinct()) {
      if (!isCanUseSetQuantifierFunction(function.getIdentifier())) {
        throw new IllegalArgumentException(
            "function " + function.getIdentifier() + " can't use DISTINCT");
      }
      // min和max无需去重
      if (!function.getIdentifier().equals(Max.MAX) && !function.getIdentifier().equals(Min.MIN)) {
        table = transformToTable(executeDistinct(table));
      }
    }

    try {
      Row row = function.transform(table, params);
      if (row == null) {
        return Table.EMPTY_TABLE;
      }
      Header header = row.getHeader();
      return new Table(header, Collections.singletonList(row));
    } catch (Exception e) {
      throw new PhysicalTaskExecuteFailureException(
          "encounter error when execute set mapping function " + function.getIdentifier() + ".", e);
    }
  }

  private RowStream executeMappingTransform(MappingTransform mappingTransform, Table table)
      throws PhysicalException {
    MappingFunction function = (MappingFunction) mappingTransform.getFunctionCall().getFunction();
    FunctionParams params = mappingTransform.getFunctionCall().getParams();
    try {
      return function.transform(table, params);
    } catch (Exception e) {
      throw new PhysicalTaskExecuteFailureException(
          "encounter error when execute mapping function " + function.getIdentifier() + ".", e);
    }
  }

  private RowStream executeRename(Rename rename, Table table) throws PhysicalException {
    Header header = table.getHeader();
    Map<String, String> aliasMap = rename.getAliasMap();

    List<Field> fields = new ArrayList<>();
    List<String> ignorePatterns = rename.getIgnorePatterns();
    header
        .getFields()
        .forEach(
            field -> {
              // 如果列名在ignorePatterns中，对该列不执行rename
              for (String ignorePattern : ignorePatterns) {
                if (ignorePattern.endsWith(ALL_PATH_SUFFIX)) {
                  if (field
                      .getName()
                      .startsWith(ignorePattern.substring(0, ignorePattern.length() - 1))) {
                    fields.add(field);
                    return;
                  }
                } else {
                  if (field.getName().equals(ignorePattern)) {
                    fields.add(field);
                    return;
                  }
                }
              }
              String alias = "";
              for (String oldName : aliasMap.keySet()) {
                if (Objects.equals(oldName, "*") && aliasMap.get(oldName).endsWith(".*")) {
                  String newPrefix = aliasMap.get(oldName).replace("*", "");
                  alias = newPrefix + field.getName();
                } else if (oldName.endsWith(".*") && aliasMap.get(oldName).endsWith(".*")) {
                  String oldPrefix = oldName.replace(".*", "");
                  String newPrefix = aliasMap.get(oldName).replace(".*", "");
                  if (field.getName().startsWith(oldPrefix)) {
                    alias = field.getName().replaceFirst(oldPrefix, newPrefix);
                  }
                  break;
                } else if (oldName.equals(field.getFullName())) {
                  alias = aliasMap.get(oldName);
                  break;
                } else {
                  Pattern pattern = Pattern.compile(StringUtils.reformatColumnName(oldName) + ".*");
                  if (pattern.matcher(field.getFullName()).matches()) {
                    alias = aliasMap.get(oldName);
                    break;
                  }
                }
              }
              if (alias.equals("")) {
                fields.add(field);
              } else {
                fields.add(new Field(alias, field.getType(), field.getTags()));
              }
            });

    Header newHeader = new Header(header.getKey(), fields);

    List<Row> rows = new ArrayList<>();
    table
        .getRows()
        .forEach(
            row -> {
              if (newHeader.hasKey()) {
                rows.add(new Row(newHeader, row.getKey(), row.getValues()));
              } else {
                rows.add(new Row(newHeader, row.getValues()));
              }
            });

    return new Table(newHeader, rows);
  }

  private RowStream executeAddSchemaPrefix(AddSchemaPrefix addSchemaPrefix, Table table)
      throws PhysicalException {
    Header header = table.getHeader();
    String schemaPrefix = addSchemaPrefix.getSchemaPrefix();

    List<Field> fields = new ArrayList<>();
    header
        .getFields()
        .forEach(
            field -> {
              if (schemaPrefix != null) {
                fields.add(
                    new Field(
                        schemaPrefix + "." + field.getName(), field.getType(), field.getTags()));
              } else {
                fields.add(new Field(field.getName(), field.getType(), field.getTags()));
              }
            });

    Header newHeader = new Header(header.getKey(), fields);

    List<Row> rows = new ArrayList<>();
    table
        .getRows()
        .forEach(
            row -> {
              if (newHeader.hasKey()) {
                rows.add(new Row(newHeader, row.getKey(), row.getValues()));
              } else {
                rows.add(new Row(newHeader, row.getValues()));
              }
            });

    return new Table(newHeader, rows);
  }

  private RowStream executeGroupBy(GroupBy groupBy, Table table) throws PhysicalException {
    List<Row> rows = RowUtils.cacheGroupByResult(groupBy, table);
    if (rows.isEmpty()) {
      return Table.EMPTY_TABLE;
    }
    Header header = rows.get(0).getHeader();
    return new Table(header, rows);
  }

  private RowStream executeReorder(Reorder reorder, Table table) throws PhysicalException {
    Header header = table.getHeader();
    List<Field> targetFields = new ArrayList<>();
    Map<Integer, Integer> reorderMap = new HashMap<>();

    for (String pattern : reorder.getPatterns()) {
      List<Pair<Field, Integer>> matchedFields = new ArrayList<>();
      if (StringUtils.isPattern(pattern)) {
        for (int i = 0; i < header.getFields().size(); i++) {
          Field field = header.getField(i);
          if (Pattern.matches(StringUtils.reformatColumnName(pattern), field.getName())) {
            matchedFields.add(new Pair<>(field, i));
          }
        }
      } else {
        for (int i = 0; i < header.getFields().size(); i++) {
          Field field = header.getField(i);
          if (pattern.equals(field.getName())) {
            matchedFields.add(new Pair<>(field, i));
          }
        }
      }
      if (!matchedFields.isEmpty()) {
        matchedFields.sort(Comparator.comparing(pair -> pair.getK().getFullName()));
        matchedFields.forEach(
            pair -> {
              reorderMap.put(targetFields.size(), pair.getV());
              targetFields.add(pair.getK());
            });
      }
    }

    Header newHeader = new Header(header.getKey(), targetFields);
    List<Row> rows = new ArrayList<>();
    table
        .getRows()
        .forEach(
            row -> {
              Object[] values = new Object[targetFields.size()];
              for (int i = 0; i < values.length; i++) {
                values[i] = row.getValue(reorderMap.get(i));
              }
              if (newHeader.hasKey()) {
                rows.add(new Row(newHeader, row.getKey(), values));
              } else {
                rows.add(new Row(newHeader, values));
              }
            });
    return new Table(newHeader, rows);
  }

  private RowStream executeDistinct(Table table) throws PhysicalException {
    if (table.getHeader().getFields().isEmpty()) {
      return table;
    }

    Header newHeader = new Header(table.getHeader().getFields());
    List<Row> targetRows = removeDuplicateRows(table.getRows());

    return new Table(newHeader, targetRows);
  }

  private RowStream executeValueToSelectedPath(ValueToSelectedPath operator, Table table) {
    String prefix = operator.getPrefix();

    int fieldSize = table.getHeader().getFieldSize();
    Header targetHeader =
        new Header(Collections.singletonList(new Field("SelectedPath", DataType.BINARY)));
    List<Row> targetRows = new ArrayList<>();
    table
        .getRows()
        .forEach(
            row -> {
              for (int i = 0; i < fieldSize; i++) {
                Object[] value = new Object[1];
                value[0] =
                    (prefix + DOT + row.getAsValue(i).getAsString())
                        .getBytes(StandardCharsets.UTF_8);
                targetRows.add(new Row(targetHeader, value));
              }
            });

    return new Table(targetHeader, targetRows);
  }

  private RowStream executeJoin(Join join, Table tableA, Table tableB) throws PhysicalException {
    boolean hasIntersect = false;
    Header headerA = tableA.getHeader();
    Header headerB = tableB.getHeader();
    // 检查 field，暂时不需要
    for (Field field : headerA.getFields()) {
      Field relatedField = headerB.getFieldByName(field.getFullName());
      if (relatedField != null) {
        if (relatedField.getType() != field.getType()) {
          throw new PhysicalException(
              "path "
                  + field.getFullName()
                  + " has two different types: "
                  + field.getType()
                  + ", "
                  + relatedField.getType());
        }
        hasIntersect = true;
      }
    }
    if (hasIntersect) {
      return executeIntersectJoin(join, tableA, tableB);
    }
    // 目前只支持使用时间戳和顺序
    if (join.getJoinBy().equals(Constants.KEY)) {
      // 检查时间戳
      if (!headerA.hasKey() || !headerB.hasKey()) {
        throw new InvalidOperatorParameterException(
            "row streams for join operator by time should have timestamp.");
      }
      List<Field> newFields = new ArrayList<>();
      newFields.addAll(headerA.getFields());
      newFields.addAll(headerB.getFields());
      Header newHeader = new Header(Field.KEY, newFields);
      List<Row> newRows = new ArrayList<>();

      int index1 = 0, index2 = 0;
      while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
        Row rowA = tableA.getRow(index1), rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        long timestamp;
        if (rowA.getKey() == rowB.getKey()) {
          timestamp = rowA.getKey();
          System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
          System.arraycopy(
              rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
          index1++;
          index2++;
        } else if (rowA.getKey() < rowB.getKey()) {
          timestamp = rowA.getKey();
          System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
          index1++;
        } else {
          timestamp = rowB.getKey();
          System.arraycopy(
              rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
          index2++;
        }
        newRows.add(new Row(newHeader, timestamp, values));
      }

      for (; index1 < tableA.getRowSize(); index1++) {
        Row rowA = tableA.getRow(index1);
        Object[] values = new Object[newHeader.getFieldSize()];
        System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
        newRows.add(new Row(newHeader, rowA.getKey(), values));
      }

      for (; index2 < tableB.getRowSize(); index2++) {
        Row rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        System.arraycopy(
            rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
        newRows.add(new Row(newHeader, rowB.getKey(), values));
      }
      return new Table(newHeader, newRows);
    } else if (join.getJoinBy().equals(Constants.ORDINAL)) {
      if (headerA.hasKey() || headerB.hasKey()) {
        throw new InvalidOperatorParameterException(
            "row streams for join operator by ordinal shouldn't have timestamp.");
      }
      List<Field> newFields = new ArrayList<>();
      newFields.addAll(headerA.getFields());
      newFields.addAll(headerB.getFields());
      Header newHeader = new Header(newFields);
      List<Row> newRows = new ArrayList<>();

      int index1 = 0, index2 = 0;
      while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
        Row rowA = tableA.getRow(index1), rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
        System.arraycopy(
            rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
        index1++;
        index2++;
        newRows.add(new Row(newHeader, values));
      }
      for (; index1 < tableA.getRowSize(); index1++) {
        Row rowA = tableA.getRow(index1);
        Object[] values = new Object[newHeader.getFieldSize()];
        System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
        newRows.add(new Row(newHeader, values));
      }

      for (; index2 < tableB.getRowSize(); index2++) {
        Row rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        System.arraycopy(
            rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
        newRows.add(new Row(newHeader, values));
      }
      return new Table(newHeader, newRows);
    } else {
      throw new InvalidOperatorParameterException(
          "join operator is not support for field "
              + join.getJoinBy()
              + " except for "
              + Constants.KEY
              + " and "
              + Constants.ORDINAL);
    }
  }

  private RowStream executeCrossJoin(CrossJoin crossJoin, Table tableA, Table tableB)
      throws PhysicalException {
    Header newHeader =
        HeaderUtils.constructNewHead(
            tableA.getHeader(), tableB.getHeader(), crossJoin.getPrefixA(), crossJoin.getPrefixB());

    List<Row> transformedRows = new ArrayList<>();
    for (Row rowA : tableA.getRows()) {
      for (Row rowB : tableB.getRows()) {
        Row joinedRow =
            RowUtils.constructNewRow(
                newHeader, rowA, rowB, crossJoin.getPrefixA(), crossJoin.getPrefixB());
        transformedRows.add(joinedRow);
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB)
      throws PhysicalException {
    switch (innerJoin.getJoinAlgType()) {
      case NestedLoopJoin:
        return executeNestedLoopInnerJoin(innerJoin, tableA, tableB);
      case HashJoin:
        return executeHashInnerJoin(innerJoin, tableA, tableB);
      case SortedMergeJoin:
        return executeSortedMergeInnerJoin(innerJoin, tableA, tableB);
      default:
        throw new PhysicalException("Unknown join algorithm type: " + innerJoin.getJoinAlgType());
    }
  }

  private RowStream executeNestedLoopInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB)
      throws PhysicalException {
    List<String> joinColumns = new ArrayList<>(innerJoin.getJoinColumns());

    // 计算自然连接的连接列名
    if (innerJoin.isNaturalJoin()) {
      RowUtils.fillNaturalJoinColumns(
          joinColumns,
          tableA.getHeader(),
          tableB.getHeader(),
          innerJoin.getPrefixA(),
          innerJoin.getPrefixB());
    }

    // 检查连接列名是否合法
    checkJoinColumns(
        joinColumns,
        tableA.getHeader(),
        tableB.getHeader(),
        innerJoin.getPrefixA(),
        innerJoin.getPrefixB());

    // 检查左右两表需要进行额外连接的path
    List<String> extraJoinPaths = new ArrayList<>();
    if (!innerJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              tableA.getHeader(), tableB.getHeader(), innerJoin.getExtraJoinPrefix());
    }

    // 计算连接之后的header
    Header newHeader =
        constructNewHead(
            tableA.getHeader(),
            tableB.getHeader(),
            innerJoin.getPrefixA(),
            innerJoin.getPrefixB(),
            true,
            joinColumns,
            extraJoinPaths);

    List<Row> transformedRows = new ArrayList<>();
    for (Row rowA : tableA.getRows()) {
      for (Row rowB : tableB.getRows()) {
        if (!equalOnSpecificPaths(rowA, rowB, extraJoinPaths)) {
          continue;
        } else if (!equalOnSpecificPaths(
            rowA, rowB, innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumns)) {
          continue;
        }
        Row joinedRow =
            RowUtils.constructNewRow(
                newHeader,
                rowA,
                rowB,
                innerJoin.getPrefixA(),
                innerJoin.getPrefixB(),
                true,
                joinColumns,
                extraJoinPaths);
        if (innerJoin.getFilter() != null) {
          if (!FilterUtils.validate(innerJoin.getFilter(), joinedRow)) {
            continue;
          }
        }
        transformedRows.add(joinedRow);
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeHashInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB)
      throws PhysicalException {
    List<String> joinColumns = new ArrayList<>(innerJoin.getJoinColumns());

    // 计算自然连接的连接列名
    if (innerJoin.isNaturalJoin()) {
      RowUtils.fillNaturalJoinColumns(
          joinColumns,
          tableA.getHeader(),
          tableB.getHeader(),
          innerJoin.getPrefixA(),
          innerJoin.getPrefixB());
    }

    // 检查连接列名是否合法
    checkJoinColumns(
        joinColumns,
        tableA.getHeader(),
        tableB.getHeader(),
        innerJoin.getPrefixA(),
        innerJoin.getPrefixB());

    // 检查左右两表需要进行额外连接的path
    List<String> extraJoinPaths = new ArrayList<>();
    if (!innerJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              tableA.getHeader(), tableB.getHeader(), innerJoin.getExtraJoinPrefix());
    }

    // 计算建立和访问哈希表所用的path
    Pair<String, String> pair =
        calculateHashJoinPath(
            tableA.getHeader(),
            tableB.getHeader(),
            innerJoin.getPrefixA(),
            innerJoin.getPrefixB(),
            innerJoin.getFilter(),
            joinColumns,
            extraJoinPaths);
    String joinPathA = pair.k;
    String joinPathB = pair.v;

    // 检查是否需要类型转换
    boolean needTypeCast =
        checkNeedTypeCast(tableA.getRows(), tableB.getRows(), joinPathA, joinPathB);

    // 扫描右表建立哈希表
    HashMap<Integer, List<Row>> rowsBHashMap =
        establishHashMap(tableB.getRows(), joinPathB, needTypeCast);

    // 计算连接之后的header
    Header newHeader =
        constructNewHead(
            tableA.getHeader(),
            tableB.getHeader(),
            innerJoin.getPrefixA(),
            innerJoin.getPrefixB(),
            true,
            joinColumns,
            extraJoinPaths);

    List<Row> transformedRows = new ArrayList<>();
    for (Row rowA : tableA.getRows()) {
      Value value = rowA.getAsValue(joinPathA);
      if (value == null) {
        continue;
      }
      int hash = getHash(value, needTypeCast);

      if (rowsBHashMap.containsKey(hash)) {
        for (Row rowB : rowsBHashMap.get(hash)) {
          if (!equalOnSpecificPaths(rowA, rowB, extraJoinPaths)) {
            continue;
          } else if (!equalOnSpecificPaths(
              rowA, rowB, innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumns)) {
            continue;
          }
          Row joinedRow =
              RowUtils.constructNewRow(
                  newHeader,
                  rowA,
                  rowB,
                  innerJoin.getPrefixA(),
                  innerJoin.getPrefixB(),
                  true,
                  joinColumns,
                  extraJoinPaths);
          if (innerJoin.getFilter() != null) {
            if (!FilterUtils.validate(innerJoin.getFilter(), joinedRow)) {
              continue;
            }
          }
          transformedRows.add(joinedRow);
        }
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeSortedMergeInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB)
      throws PhysicalException {
    Filter filter = innerJoin.getFilter();
    List<String> joinColumns = new ArrayList<>(innerJoin.getJoinColumns());
    if (innerJoin.isNaturalJoin()) {
      RowUtils.fillNaturalJoinColumns(
          joinColumns,
          tableA.getHeader(),
          tableB.getHeader(),
          innerJoin.getPrefixA(),
          innerJoin.getPrefixB());
    }
    if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
      throw new InvalidOperatorParameterException(
          "using(or natural) and on operator cannot be used at the same time");
    }
    Header headerA = tableA.getHeader();
    Header headerB = tableB.getHeader();

    List<Row> rowsA = tableA.getRows();
    List<Row> rowsB = tableB.getRows();
    List<String> joinColumnsA = new ArrayList<>(joinColumns);
    List<String> joinColumnsB = new ArrayList<>(joinColumns);
    if (filter != null) {
      joinColumnsA = new ArrayList<>();
      joinColumnsB = new ArrayList<>();
      List<Pair<String, String>> pairs = FilterUtils.getJoinColumnsFromFilter(filter);
      if (pairs.isEmpty()) {
        throw new InvalidOperatorParameterException(
            "on condition in join operator has no join columns.");
      }
      for (Pair<String, String> p : pairs) {
        if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
          joinColumnsA.add(p.k.replaceFirst(innerJoin.getPrefixA() + '.', ""));
          joinColumnsB.add(p.v.replaceFirst(innerJoin.getPrefixB() + '.', ""));
        } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
          joinColumnsA.add(p.v.replaceFirst(innerJoin.getPrefixA() + '.', ""));
          joinColumnsB.add(p.k.replaceFirst(innerJoin.getPrefixB() + '.', ""));
        } else {
          throw new InvalidOperatorParameterException("invalid join path filter input.");
        }
      }
    }

    boolean isAscendingSorted;
    int flagA = RowUtils.checkRowsSortedByColumns(rowsA, innerJoin.getPrefixA(), joinColumnsA);
    int flagB = RowUtils.checkRowsSortedByColumns(rowsB, innerJoin.getPrefixB(), joinColumnsB);
    if (flagA == -1 || flagB == -1) {
      throw new InvalidOperatorParameterException("input rows in merge join haven't be sorted.");
    } else if (flagA + flagB == 3) {
      throw new InvalidOperatorParameterException(
          "input two rows in merge join shouldn't have different sort order.");
    } else if ((flagA == flagB)) {
      isAscendingSorted = flagA == 0 || flagA == 1;
    } else {
      isAscendingSorted = flagA == 1 || flagB == 1;
    }
    if (!isAscendingSorted) {
      for (int index = 0; index < rowsA.size(); index++) {
        rowsA.set(index, tableA.getRow(rowsA.size() - index - 1));
      }
      for (int index = 0; index < rowsB.size(); index++) {
        rowsB.set(index, tableB.getRow(rowsB.size() - index - 1));
      }
    }

    Header newHeader;
    List<Row> transformedRows = new ArrayList<>();
    if (filter != null) {
      newHeader =
          HeaderUtils.constructNewHead(
              headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());
      int indexA = 0;
      int indexB = 0;
      int startIndexOfContinuousEqualValuesB = 0;
      while (indexA < rowsA.size() && indexB < rowsB.size()) {
        int flagAEqualB =
            RowUtils.compareRowsSortedByColumns(
                rowsA.get(indexA),
                rowsB.get(indexB),
                innerJoin.getPrefixA(),
                innerJoin.getPrefixB(),
                joinColumnsA,
                joinColumnsB);
        if (flagAEqualB == 0) {
          Row joinedRow =
              RowUtils.constructNewRow(
                  newHeader,
                  rowsA.get(indexA),
                  rowsB.get(indexB),
                  innerJoin.getPrefixA(),
                  innerJoin.getPrefixB());
          if (FilterUtils.validate(filter, joinedRow)) {
            transformedRows.add(joinedRow);
          }

          if (indexA + 1 == rowsA.size()) {
            if (indexB + 1 == rowsB.size()) {
              break;
            } else {
              indexB++;
            }
          } else {
            if (indexB + 1 == rowsB.size()) {
              indexA++;
              indexB = startIndexOfContinuousEqualValuesB;
            } else {
              int flagAEqualNextA =
                  RowUtils.compareRowsSortedByColumns(
                      rowsA.get(indexA),
                      rowsA.get(indexA + 1),
                      innerJoin.getPrefixA(),
                      innerJoin.getPrefixA(),
                      joinColumnsA,
                      joinColumnsA);
              int flagBEqualNextB =
                  RowUtils.compareRowsSortedByColumns(
                      rowsB.get(indexB),
                      rowsB.get(indexB + 1),
                      innerJoin.getPrefixB(),
                      innerJoin.getPrefixB(),
                      joinColumnsB,
                      joinColumnsB);
              if (flagBEqualNextB == 0) {
                indexB++;
              } else {
                indexA++;
                if (flagAEqualNextA != 0) {
                  indexB++;
                  startIndexOfContinuousEqualValuesB = indexB;
                } else {
                  indexB = startIndexOfContinuousEqualValuesB;
                }
              }
            }
          }
        } else if (flagAEqualB == -1) {
          indexA++;
        } else {
          indexB++;
          startIndexOfContinuousEqualValuesB = indexB;
        }
      }
    } else { // Join condition: natural or using
      Pair<int[], Header> pair =
          HeaderUtils.constructNewHead(
              headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumns, true);
      int[] indexOfJoinColumnInTableB = pair.getK();
      newHeader = pair.getV();
      int indexA = 0;
      int indexB = 0;
      int startIndexOfContinuousEqualValuesB = 0;
      while (indexA < rowsA.size() && indexB < rowsB.size()) {
        int flagAEqualB =
            RowUtils.compareRowsSortedByColumns(
                rowsA.get(indexA),
                rowsB.get(indexB),
                innerJoin.getPrefixA(),
                innerJoin.getPrefixB(),
                joinColumnsA,
                joinColumnsB);
        if (flagAEqualB == 0) {
          Row joinedRow =
              RowUtils.constructNewRow(
                  newHeader, rowsA.get(indexA), rowsB.get(indexB), indexOfJoinColumnInTableB, true);
          transformedRows.add(joinedRow);

          if (indexA + 1 == rowsA.size()) {
            if (indexB + 1 == rowsB.size()) {
              break;
            } else {
              indexB++;
            }
          } else {
            if (indexB + 1 == rowsB.size()) {
              indexA++;
              indexB = startIndexOfContinuousEqualValuesB;
            } else {
              int flagAEqualNextA =
                  RowUtils.compareRowsSortedByColumns(
                      rowsA.get(indexA),
                      rowsA.get(indexA + 1),
                      innerJoin.getPrefixA(),
                      innerJoin.getPrefixA(),
                      joinColumnsA,
                      joinColumnsA);
              int flagBEqualNextB =
                  RowUtils.compareRowsSortedByColumns(
                      rowsB.get(indexB),
                      rowsB.get(indexB + 1),
                      innerJoin.getPrefixB(),
                      innerJoin.getPrefixB(),
                      joinColumnsB,
                      joinColumnsB);
              if (flagBEqualNextB == 0) {
                indexB++;
              } else {
                indexA++;
                if (flagAEqualNextA != 0) {
                  indexB++;
                  startIndexOfContinuousEqualValuesB = indexB;
                } else {
                  indexB = startIndexOfContinuousEqualValuesB;
                }
              }
            }
          }
        } else if (flagAEqualB == -1) {
          indexA++;
        } else {
          indexB++;
          startIndexOfContinuousEqualValuesB = indexB;
        }
      }
    }
    List<Row> reverseRows = new ArrayList<>(transformedRows);
    if (!isAscendingSorted) {
      for (int index = 0; index < reverseRows.size(); index++) {
        transformedRows.set(index, reverseRows.get(reverseRows.size() - index - 1));
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB)
      throws PhysicalException {
    switch (outerJoin.getJoinAlgType()) {
      case NestedLoopJoin:
        return executeNestedLoopOuterJoin(outerJoin, tableA, tableB);
      case HashJoin:
        return executeHashOuterJoin(outerJoin, tableA, tableB);
      case SortedMergeJoin:
        return executeSortedMergeOuterJoin(outerJoin, tableA, tableB);
      default:
        throw new PhysicalException("Unknown join algorithm type: " + outerJoin.getJoinAlgType());
    }
  }

  private RowStream executeNestedLoopOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB)
      throws PhysicalException {
    OuterJoinType outerType = outerJoin.getOuterJoinType();
    List<String> joinColumns = new ArrayList<>(outerJoin.getJoinColumns());

    // 计算自然连接的连接列名
    if (outerJoin.isNaturalJoin()) {
      RowUtils.fillNaturalJoinColumns(
          joinColumns,
          tableA.getHeader(),
          tableB.getHeader(),
          outerJoin.getPrefixA(),
          outerJoin.getPrefixB());
    }

    // 检查连接列名是否合法
    checkJoinColumns(
        joinColumns,
        tableA.getHeader(),
        tableB.getHeader(),
        outerJoin.getPrefixA(),
        outerJoin.getPrefixB());

    // 检查左右两表需要进行额外连接的path
    List<String> extraJoinPaths = new ArrayList<>();
    if (!outerJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              tableA.getHeader(), tableB.getHeader(), outerJoin.getExtraJoinPrefix());
    }

    List<Row> rowsA = tableA.getRows();
    List<Row> rowsB = tableB.getRows();
    Bitmap bitmapA = new Bitmap(rowsA.size());
    Bitmap bitmapB = new Bitmap(rowsB.size());

    boolean cutRight = !outerJoin.getOuterJoinType().equals(OuterJoinType.RIGHT);

    // 计算连接之后的header
    Header newHeader =
        constructNewHead(
            tableA.getHeader(),
            tableB.getHeader(),
            outerJoin.getPrefixA(),
            outerJoin.getPrefixB(),
            cutRight,
            joinColumns,
            extraJoinPaths);

    List<Row> transformedRows = new ArrayList<>();
    for (int indexA = 0; indexA < rowsA.size(); indexA++) {
      for (int indexB = 0; indexB < rowsB.size(); indexB++) {
        if (!equalOnSpecificPaths(rowsA.get(indexA), rowsB.get(indexB), extraJoinPaths)) {
          continue;
        } else if (!equalOnSpecificPaths(
            rowsA.get(indexA),
            rowsB.get(indexB),
            outerJoin.getPrefixA(),
            outerJoin.getPrefixB(),
            joinColumns)) {
          continue;
        }
        Row joinedRow =
            RowUtils.constructNewRow(
                newHeader,
                rowsA.get(indexA),
                rowsB.get(indexB),
                outerJoin.getPrefixA(),
                outerJoin.getPrefixB(),
                cutRight,
                joinColumns,
                extraJoinPaths);
        if (outerJoin.getFilter() != null) {
          if (!FilterUtils.validate(outerJoin.getFilter(), joinedRow)) {
            continue;
          }
        }
        if (!bitmapA.get(indexA)) {
          bitmapA.mark(indexA);
        }
        if (!bitmapB.get(indexB)) {
          bitmapB.mark(indexB);
        }
        transformedRows.add(joinedRow);
      }
    }
    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
      int anotherRowSize =
          tableB.getHeader().hasKey() && outerJoin.getPrefixB() != null
              ? rowsB.get(0).getValues().length + 1
              : rowsB.get(0).getValues().length;
      anotherRowSize -= joinColumns.size();
      anotherRowSize -= extraJoinPaths.size();

      for (int i = 0; i < rowsA.size(); i++) {
        if (!bitmapA.get(i)) {
          Row unmatchedRow =
              RowUtils.constructUnmatchedRow(
                  newHeader, rowsA.get(i), outerJoin.getPrefixA(), anotherRowSize, true);
          transformedRows.add(unmatchedRow);
        }
      }
    }
    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
      int anotherRowSize =
          tableA.getHeader().hasKey() && outerJoin.getPrefixA() != null
              ? rowsA.get(0).getValues().length + 1
              : rowsA.get(0).getValues().length;
      anotherRowSize -= joinColumns.size();
      anotherRowSize -= extraJoinPaths.size();

      for (int i = 0; i < rowsB.size(); i++) {
        if (!bitmapB.get(i)) {
          Row unmatchedRow =
              RowUtils.constructUnmatchedRow(
                  newHeader, rowsB.get(i), outerJoin.getPrefixB(), anotherRowSize, false);
          transformedRows.add(unmatchedRow);
        }
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeHashOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB)
      throws PhysicalException {
    OuterJoinType outerType = outerJoin.getOuterJoinType();
    List<String> joinColumns = new ArrayList<>(outerJoin.getJoinColumns());

    // 计算自然连接的连接列名
    if (outerJoin.isNaturalJoin()) {
      RowUtils.fillNaturalJoinColumns(
          joinColumns,
          tableA.getHeader(),
          tableB.getHeader(),
          outerJoin.getPrefixA(),
          outerJoin.getPrefixB());
    }

    // 检查连接列名是否合法
    checkJoinColumns(
        joinColumns,
        tableA.getHeader(),
        tableB.getHeader(),
        outerJoin.getPrefixA(),
        outerJoin.getPrefixB());

    // 检查左右两表需要进行额外连接的path
    List<String> extraJoinPaths = new ArrayList<>();
    if (!outerJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              tableA.getHeader(), tableB.getHeader(), outerJoin.getExtraJoinPrefix());
    }

    // 计算建立和访问哈希表所用的path
    Pair<String, String> pair =
        calculateHashJoinPath(
            tableA.getHeader(),
            tableB.getHeader(),
            outerJoin.getPrefixA(),
            outerJoin.getPrefixB(),
            outerJoin.getFilter(),
            joinColumns,
            extraJoinPaths);
    String joinPathA = pair.k;
    String joinPathB = pair.v;

    // 检查是否需要类型转换
    boolean needTypeCast =
        checkNeedTypeCast(tableA.getRows(), tableB.getRows(), joinPathA, joinPathB);

    List<Row> rowsA = tableA.getRows();
    List<Row> rowsB = tableB.getRows();
    Bitmap bitmapA = new Bitmap(rowsA.size());
    Bitmap bitmapB = new Bitmap(rowsB.size());

    HashMap<Integer, List<Row>> rowsBHashMap = new HashMap<>();
    HashMap<Integer, List<Integer>> indexOfRowBHashMap = new HashMap<>();
    for (int indexB = 0; indexB < rowsB.size(); indexB++) {
      Value value = rowsB.get(indexB).getAsValue(joinPathB);
      if (value == null) {
        continue;
      }
      int hash = getHash(value, needTypeCast);
      List<Row> l = rowsBHashMap.computeIfAbsent(hash, k -> new ArrayList<>());
      List<Integer> il = indexOfRowBHashMap.computeIfAbsent(hash, k -> new ArrayList<>());
      l.add(rowsB.get(indexB));
      il.add(indexB);
    }

    boolean cutRight = !outerJoin.getOuterJoinType().equals(OuterJoinType.RIGHT);

    // 计算连接之后的header
    Header newHeader =
        constructNewHead(
            tableA.getHeader(),
            tableB.getHeader(),
            outerJoin.getPrefixA(),
            outerJoin.getPrefixB(),
            cutRight,
            joinColumns,
            extraJoinPaths);

    List<Row> transformedRows = new ArrayList<>();

    for (int indexA = 0; indexA < rowsA.size(); indexA++) {
      Value value = rowsA.get(indexA).getAsValue(joinPathA);
      if (value == null) {
        continue;
      }
      int hash = getHash(value, needTypeCast);

      if (rowsBHashMap.containsKey(hash)) {
        List<Row> hashRowsB = rowsBHashMap.get(hash);
        List<Integer> hashIndexB = indexOfRowBHashMap.get(hash);
        for (int i = 0; i < hashRowsB.size(); i++) {
          int indexB = hashIndexB.get(i);
          if (!equalOnSpecificPaths(rowsA.get(indexA), hashRowsB.get(i), extraJoinPaths)) {
            continue;
          } else if (!equalOnSpecificPaths(
              rowsA.get(indexA),
              hashRowsB.get(i),
              outerJoin.getPrefixA(),
              outerJoin.getPrefixB(),
              joinColumns)) {
            continue;
          }
          Row joinedRow =
              RowUtils.constructNewRow(
                  newHeader,
                  rowsA.get(indexA),
                  hashRowsB.get(i),
                  outerJoin.getPrefixA(),
                  outerJoin.getPrefixB(),
                  cutRight,
                  joinColumns,
                  extraJoinPaths);
          if (outerJoin.getFilter() != null) {
            if (!FilterUtils.validate(outerJoin.getFilter(), joinedRow)) {
              continue;
            }
          }
          if (!bitmapA.get(indexA)) {
            bitmapA.mark(indexA);
          }
          if (!bitmapB.get(indexB)) {
            bitmapB.mark(indexB);
          }
          transformedRows.add(joinedRow);
        }
      }
    }
    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
      int anotherRowSize =
          tableB.getHeader().hasKey() && outerJoin.getPrefixB() != null
              ? rowsB.get(0).getValues().length + 1
              : rowsB.get(0).getValues().length;
      anotherRowSize -= joinColumns.size();
      anotherRowSize -= extraJoinPaths.size();

      for (int i = 0; i < rowsA.size(); i++) {
        if (!bitmapA.get(i)) {
          Row unMatchedRow =
              RowUtils.constructUnmatchedRow(
                  newHeader, rowsA.get(i), outerJoin.getPrefixA(), anotherRowSize, true);
          transformedRows.add(unMatchedRow);
        }
      }
    }
    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
      int anotherRowSize =
          tableA.getHeader().hasKey() && outerJoin.getPrefixA() != null
              ? rowsA.get(0).getValues().length + 1
              : rowsA.get(0).getValues().length;
      anotherRowSize -= joinColumns.size();
      anotherRowSize -= extraJoinPaths.size();

      for (int i = 0; i < rowsB.size(); i++) {
        if (!bitmapB.get(i)) {
          Row unMatchedRow =
              RowUtils.constructUnmatchedRow(
                  newHeader, rowsB.get(i), outerJoin.getPrefixB(), anotherRowSize, false);
          transformedRows.add(unMatchedRow);
        }
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeSortedMergeOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB)
      throws PhysicalException {
    OuterJoinType outerType = outerJoin.getOuterJoinType();
    Filter filter = outerJoin.getFilter();
    List<String> joinColumns = new ArrayList<>(outerJoin.getJoinColumns());
    List<Field> fieldsA = new ArrayList<>(tableA.getHeader().getFields());
    List<Field> fieldsB = new ArrayList<>(tableB.getHeader().getFields());
    if (outerJoin.isNaturalJoin()) {
      if (!joinColumns.isEmpty()) {
        throw new InvalidOperatorParameterException(
            "natural inner join operator should not have using operator");
      }
      for (Field fieldA : fieldsA) {
        for (Field fieldB : fieldsB) {
          String joinColumnA = fieldA.getName().replaceFirst(outerJoin.getPrefixA() + '.', "");
          String joinColumnB = fieldB.getName().replaceFirst(outerJoin.getPrefixB() + '.', "");
          if (joinColumnA.equals(joinColumnB)) {
            joinColumns.add(joinColumnA);
          }
        }
      }
      if (joinColumns.isEmpty()) {
        throw new PhysicalException("natural join has no matching columns");
      }
    }
    if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
      throw new InvalidOperatorParameterException(
          "using(or natural) and on operator cannot be used at the same time");
    }
    Header headerA = tableA.getHeader();
    Header headerB = tableB.getHeader();

    List<Row> rowsA = tableA.getRows();
    List<Row> rowsB = tableB.getRows();
    List<String> joinColumnsA = new ArrayList<>(joinColumns);
    List<String> joinColumnsB = new ArrayList<>(joinColumns);
    if (filter != null) {
      joinColumnsA = new ArrayList<>();
      joinColumnsB = new ArrayList<>();
      List<Pair<String, String>> pairs = FilterUtils.getJoinColumnsFromFilter(filter);
      if (pairs.isEmpty()) {
        throw new InvalidOperatorParameterException(
            "on condition in join operator has no join columns.");
      }
      for (Pair<String, String> p : pairs) {
        if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
          joinColumnsA.add(p.k.replaceFirst(outerJoin.getPrefixA() + '.', ""));
          joinColumnsB.add(p.v.replaceFirst(outerJoin.getPrefixB() + '.', ""));
        } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
          joinColumnsA.add(p.v.replaceFirst(outerJoin.getPrefixA() + '.', ""));
          joinColumnsB.add(p.k.replaceFirst(outerJoin.getPrefixB() + '.', ""));
        } else {
          throw new InvalidOperatorParameterException("invalid join path filter input.");
        }
      }
    }

    boolean isAscendingSorted;
    int flagA = RowUtils.checkRowsSortedByColumns(rowsA, outerJoin.getPrefixA(), joinColumnsA);
    int flagB = RowUtils.checkRowsSortedByColumns(rowsB, outerJoin.getPrefixB(), joinColumnsB);
    if (flagA == -1 || flagB == -1) {
      throw new InvalidOperatorParameterException("input rows in merge join haven't be sorted.");
    } else if (flagA + flagB == 3) {
      throw new InvalidOperatorParameterException(
          "input two rows in merge join shouldn't have different sort order.");
    } else if ((flagA == flagB)) {
      isAscendingSorted = flagA == 0 || flagA == 1;
    } else {
      isAscendingSorted = flagA == 1 || flagB == 1;
    }
    if (!isAscendingSorted) {
      for (int index = 0; index < rowsA.size(); index++) {
        rowsA.set(index, tableA.getRow(rowsA.size() - index - 1));
      }
      for (int index = 0; index < rowsB.size(); index++) {
        rowsB.set(index, tableB.getRow(rowsB.size() - index - 1));
      }
    }

    Bitmap bitmapA = new Bitmap(rowsA.size());
    Bitmap bitmapB = new Bitmap(rowsB.size());

    Header newHeader;
    List<Row> transformedRows = new ArrayList<>();
    if (filter != null) {
      newHeader =
          HeaderUtils.constructNewHead(
              headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());
      int indexA = 0;
      int indexB = 0;
      int startIndexOfContinuousEqualValuesB = 0;
      while (indexA < rowsA.size() && indexB < rowsB.size()) {
        int flagAEqualB =
            RowUtils.compareRowsSortedByColumns(
                rowsA.get(indexA),
                rowsB.get(indexB),
                outerJoin.getPrefixA(),
                outerJoin.getPrefixB(),
                joinColumnsA,
                joinColumnsB);
        if (flagAEqualB == 0) {
          Row joinedRow =
              RowUtils.constructNewRow(
                  newHeader,
                  rowsA.get(indexA),
                  rowsB.get(indexB),
                  outerJoin.getPrefixA(),
                  outerJoin.getPrefixB());
          if (FilterUtils.validate(filter, joinedRow)) {
            if (!bitmapA.get(indexA)) {
              bitmapA.mark(indexA);
            }
            if (!bitmapB.get(indexB)) {
              bitmapB.mark(indexB);
            }
            transformedRows.add(joinedRow);
          }

          if (indexA + 1 == rowsA.size()) {
            if (indexB + 1 == rowsB.size()) {
              break;
            } else {
              indexB++;
            }
          } else {
            if (indexB + 1 == rowsB.size()) {
              indexA++;
              indexB = startIndexOfContinuousEqualValuesB;
            } else {
              int flagAEqualNextA =
                  RowUtils.compareRowsSortedByColumns(
                      rowsA.get(indexA),
                      rowsA.get(indexA + 1),
                      outerJoin.getPrefixA(),
                      outerJoin.getPrefixA(),
                      joinColumnsA,
                      joinColumnsA);
              int flagBEqualNextB =
                  RowUtils.compareRowsSortedByColumns(
                      rowsB.get(indexB),
                      rowsB.get(indexB + 1),
                      outerJoin.getPrefixB(),
                      outerJoin.getPrefixB(),
                      joinColumnsB,
                      joinColumnsB);
              if (flagBEqualNextB == 0) {
                indexB++;
              } else {
                indexA++;
                if (flagAEqualNextA != 0) {
                  indexB++;
                  startIndexOfContinuousEqualValuesB = indexB;
                } else {
                  indexB = startIndexOfContinuousEqualValuesB;
                }
              }
            }
          }
        } else if (flagAEqualB == -1) {
          indexA++;
        } else {
          indexB++;
          startIndexOfContinuousEqualValuesB = indexB;
        }
      }
    } else { // Join condition: natural or using
      Pair<int[], Header> pair;
      if (outerType == OuterJoinType.RIGHT) {
        pair =
            HeaderUtils.constructNewHead(
                headerA,
                headerB,
                outerJoin.getPrefixA(),
                outerJoin.getPrefixB(),
                joinColumns,
                false);
      } else {
        pair =
            HeaderUtils.constructNewHead(
                headerA,
                headerB,
                outerJoin.getPrefixA(),
                outerJoin.getPrefixB(),
                joinColumns,
                true);
      }
      int[] indexOfJoinColumnInTable = pair.getK();
      newHeader = pair.getV();
      int indexA = 0;
      int indexB = 0;
      int startIndexOfContinuousEqualValuesB = 0;
      while (indexA < rowsA.size() && indexB < rowsB.size()) {
        int flagAEqualB =
            RowUtils.compareRowsSortedByColumns(
                rowsA.get(indexA),
                rowsB.get(indexB),
                outerJoin.getPrefixA(),
                outerJoin.getPrefixB(),
                joinColumnsA,
                joinColumnsB);
        if (flagAEqualB == 0) {
          Row joinedRow;
          if (outerType == OuterJoinType.RIGHT) {
            joinedRow =
                RowUtils.constructNewRow(
                    newHeader,
                    rowsA.get(indexA),
                    rowsB.get(indexB),
                    indexOfJoinColumnInTable,
                    false);
          } else {
            joinedRow =
                RowUtils.constructNewRow(
                    newHeader,
                    rowsA.get(indexA),
                    rowsB.get(indexB),
                    indexOfJoinColumnInTable,
                    true);
          }

          if (!bitmapA.get(indexA)) {
            bitmapA.mark(indexA);
          }
          if (!bitmapB.get(indexB)) {
            bitmapB.mark(indexB);
          }
          transformedRows.add(joinedRow);

          int flagAEqualNextA =
              RowUtils.compareRowsSortedByColumns(
                  rowsA.get(indexA),
                  rowsA.get(indexA + 1),
                  outerJoin.getPrefixA(),
                  outerJoin.getPrefixA(),
                  joinColumnsA,
                  joinColumnsA);
          int flagBEqualNextB =
              RowUtils.compareRowsSortedByColumns(
                  rowsB.get(indexB),
                  rowsB.get(indexB + 1),
                  outerJoin.getPrefixB(),
                  outerJoin.getPrefixB(),
                  joinColumnsB,
                  joinColumnsB);
          if (flagBEqualNextB == 0) {
            indexB++;
          } else {
            indexA++;
            if (flagAEqualNextA != 0) {
              indexB++;
              startIndexOfContinuousEqualValuesB = indexB;
            } else {
              indexB = startIndexOfContinuousEqualValuesB;
            }
          }
        } else if (flagAEqualB == -1) {
          indexA++;
        } else {
          indexB++;
          startIndexOfContinuousEqualValuesB = indexB;
        }
      }
    }
    List<Row> reverseRows = new ArrayList<>(transformedRows);
    if (!isAscendingSorted) {
      for (int index = 0; index < reverseRows.size(); index++) {
        transformedRows.set(index, reverseRows.get(reverseRows.size() - index - 1));
      }
    }

    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
      int anotherRowSize =
          headerB.hasKey() ? rowsB.get(0).getValues().length + 1 : rowsB.get(0).getValues().length;
      if (filter == null) {
        anotherRowSize -= joinColumns.size();
      }
      for (int i = 0; i < rowsA.size(); i++) {
        if (!bitmapA.get(i)) {
          Row unMatchedRow =
              RowUtils.constructUnmatchedRow(
                  newHeader, rowsA.get(i), outerJoin.getPrefixA(), anotherRowSize, true);
          transformedRows.add(unMatchedRow);
        }
      }
    }
    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
      int anotherRowSize =
          headerA.hasKey() ? rowsA.get(0).getValues().length + 1 : rowsA.get(0).getValues().length;
      if (filter == null) {
        anotherRowSize -= joinColumns.size();
      }
      for (int i = 0; i < rowsB.size(); i++) {
        if (!bitmapB.get(i)) {
          Row unMatchedRow =
              RowUtils.constructUnmatchedRow(
                  newHeader, rowsB.get(i), outerJoin.getPrefixB(), anotherRowSize, false);
          transformedRows.add(unMatchedRow);
        }
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeSingleJoin(SingleJoin singleJoin, Table tableA, Table tableB)
      throws PhysicalException {
    switch (singleJoin.getJoinAlgType()) {
      case NestedLoopJoin:
        return executeNestedLoopSingleJoin(singleJoin, tableA, tableB);
      case HashJoin:
        return executeHashSingleJoin(singleJoin, tableA, tableB);
      default:
        throw new PhysicalException(
            "Unsupported single join algorithm type: " + singleJoin.getJoinAlgType());
    }
  }

  private RowStream executeNestedLoopSingleJoin(SingleJoin singleJoin, Table tableA, Table tableB)
      throws PhysicalException {
    // 检查左右两表需要进行额外连接的path
    List<String> extraJoinPaths = new ArrayList<>();
    if (!singleJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              tableA.getHeader(), tableB.getHeader(), singleJoin.getExtraJoinPrefix());
    }

    Header newHeader =
        constructNewHead(tableA.getHeader(), tableB.getHeader(), true, extraJoinPaths);

    List<Row> transformedRows = new ArrayList<>();
    Filter filter = singleJoin.getFilter();
    boolean matched;
    int anotherRowSize = tableB.getHeader().getFieldSize();
    for (Row rowA : tableA.getRows()) {
      matched = false;
      for (Row rowB : tableB.getRows()) {
        if (!equalOnSpecificPaths(rowA, rowB, extraJoinPaths)) {
          continue;
        }
        Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, rowB, true);
        if (singleJoin.getFilter() != null) {
          if (!FilterUtils.validate(singleJoin.getFilter(), joinedRow)) {
            continue;
          }
        }
        if (matched) {
          throw new PhysicalException("the return value of sub-query has more than one rows");
        }
        matched = true;
        transformedRows.add(joinedRow);
      }
      if (!matched) {
        Row unmatchedRow =
            RowUtils.constructUnmatchedRow(
                newHeader, rowA, singleJoin.getPrefixA(), anotherRowSize, true);
        transformedRows.add(unmatchedRow);
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeHashSingleJoin(SingleJoin singleJoin, Table tableA, Table tableB)
      throws PhysicalException {
    // 检查左右两表需要进行额外连接的path
    List<String> extraJoinPaths = new ArrayList<>();
    if (!singleJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              tableA.getHeader(), tableB.getHeader(), singleJoin.getExtraJoinPrefix());
    }

    // 计算建立和访问哈希表所用的path
    Pair<String, String> pair =
        calculateHashJoinPath(
            tableA.getHeader(),
            tableB.getHeader(),
            singleJoin.getPrefixA(),
            singleJoin.getPrefixB(),
            singleJoin.getFilter(),
            new ArrayList<>(),
            extraJoinPaths);
    String joinPathA = pair.k;
    String joinPathB = pair.v;

    // 检查是否需要类型转换
    boolean needTypeCast =
        checkNeedTypeCast(tableA.getRows(), tableB.getRows(), joinPathA, joinPathB);

    // 扫描右表建立哈希表
    HashMap<Integer, List<Row>> rowsBHashMap =
        establishHashMap(tableB.getRows(), joinPathB, needTypeCast);

    Header newHeader =
        constructNewHead(tableA.getHeader(), tableB.getHeader(), true, extraJoinPaths);

    List<Row> transformedRows = new ArrayList<>();
    int anotherRowSize = tableB.getHeader().getFieldSize();
    for (Row rowA : tableA.getRows()) {
      Value value = rowA.getAsValue(joinPathA);
      if (value == null) {
        continue;
      }
      int hash = getHash(value, needTypeCast);

      boolean matched = false;
      if (rowsBHashMap.containsKey(hash)) {
        for (Row rowB : rowsBHashMap.get(hash)) {
          if (!equalOnSpecificPaths(rowA, rowB, extraJoinPaths)) {
            continue;
          }
          Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, rowB, true, extraJoinPaths);
          if (singleJoin.getFilter() != null) {
            if (!FilterUtils.validate(singleJoin.getFilter(), joinedRow)) {
              continue;
            }
          }
          if (matched) {
            throw new PhysicalException("the return value of sub-query has more than one rows");
          }
          matched = true;
          transformedRows.add(joinedRow);
        }
      }
      if (!matched) {
        Row unmatchedRow =
            RowUtils.constructUnmatchedRow(
                newHeader, rowA, singleJoin.getPrefixA(), anotherRowSize, true);
        transformedRows.add(unmatchedRow);
      }
    }
    return new Table(newHeader, transformedRows);
  }

  private RowStream executeMarkJoin(MarkJoin markJoin, Table tableA, Table tableB)
      throws PhysicalException {
    switch (markJoin.getJoinAlgType()) {
      case NestedLoopJoin:
        return executeNestedLoopMarkJoin(markJoin, tableA, tableB);
      case HashJoin:
        return executeHashMarkJoin(markJoin, tableA, tableB);
      default:
        throw new PhysicalException(
            "Unsupported mark join algorithm type: " + markJoin.getJoinAlgType());
    }
  }

  private RowStream executeNestedLoopMarkJoin(MarkJoin markJoin, Table tableA, Table tableB)
      throws PhysicalException {
    // 检查左右两表需要进行额外连接的path
    List<String> extraJoinPaths = new ArrayList<>();
    if (!markJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              tableA.getHeader(), tableB.getHeader(), markJoin.getExtraJoinPrefix());
    }

    Header targetHeader = constructNewHead(tableA.getHeader(), markJoin.getMarkColumn());
    Header joinHeader = constructNewHead(tableA.getHeader(), tableB.getHeader(), true);

    List<Row> transformedRows = new ArrayList<>();
    tableScan:
    for (Row rowA : tableA.getRows()) {
      for (Row rowB : tableB.getRows()) {
        if (!equalOnSpecificPaths(rowA, rowB, extraJoinPaths)) {
          continue;
        }
        Row joinedRow = RowUtils.constructNewRow(joinHeader, rowA, rowB, true);
        if (markJoin.getFilter() != null) {
          if (!FilterUtils.validate(markJoin.getFilter(), joinedRow)) {
            continue;
          }
        }
        Row returnRow =
            RowUtils.constructNewRowWithMark(targetHeader, rowA, !markJoin.isAntiJoin());
        transformedRows.add(returnRow);
        continue tableScan;
      }
      Row unmatchedRow =
          RowUtils.constructNewRowWithMark(targetHeader, rowA, markJoin.isAntiJoin());
      transformedRows.add(unmatchedRow);
    }
    return new Table(targetHeader, transformedRows);
  }

  private RowStream executeHashMarkJoin(MarkJoin markJoin, Table tableA, Table tableB)
      throws PhysicalException {
    // 检查左右两表需要进行额外连接的path
    List<String> extraJoinPaths = new ArrayList<>();
    if (!markJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              tableA.getHeader(), tableB.getHeader(), markJoin.getExtraJoinPrefix());
    }

    // 计算建立和访问哈希表所用的path
    Pair<String, String> pair =
        calculateHashJoinPath(
            tableA.getHeader(),
            tableB.getHeader(),
            markJoin.getPrefixA(),
            markJoin.getPrefixB(),
            markJoin.getFilter(),
            new ArrayList<>(),
            extraJoinPaths);
    String joinPathA = pair.k;
    String joinPathB = pair.v;

    // 检查是否需要类型转换
    boolean needTypeCast =
        checkNeedTypeCast(tableA.getRows(), tableB.getRows(), joinPathA, joinPathB);

    // 扫描右表建立哈希表
    HashMap<Integer, List<Row>> rowsBHashMap =
        establishHashMap(tableB.getRows(), joinPathB, needTypeCast);

    // 计算连接之后的header
    Header newHeader = constructNewHead(tableA.getHeader(), markJoin.getMarkColumn());
    Header joinHeader = constructNewHead(tableA.getHeader(), tableB.getHeader(), true);

    List<Row> transformedRows = new ArrayList<>();
    tableScan:
    for (Row rowA : tableA.getRows()) {
      Value value = rowA.getAsValue(joinPathA);
      if (value == null) {
        continue;
      }
      int hash = getHash(value, needTypeCast);

      if (rowsBHashMap.containsKey(hash)) {
        for (Row rowB : rowsBHashMap.get(hash)) {
          if (!equalOnSpecificPaths(rowA, rowB, extraJoinPaths)) {
            continue;
          }
          Row joinedRow = RowUtils.constructNewRow(joinHeader, rowA, rowB, true);
          if (markJoin.getFilter() != null) {
            if (!FilterUtils.validate(markJoin.getFilter(), joinedRow)) {
              continue;
            }
          }
          Row returnRow = RowUtils.constructNewRowWithMark(newHeader, rowA, !markJoin.isAntiJoin());
          transformedRows.add(returnRow);
          continue tableScan;
        }
      }
      Row unmatchedRow = RowUtils.constructNewRowWithMark(newHeader, rowA, markJoin.isAntiJoin());
      transformedRows.add(unmatchedRow);
    }
    return new Table(newHeader, transformedRows);
  }

  private static void writeToNewRow(Object[] values, Row row, Map<Field, Integer> fieldIndices) {
    List<Field> fields = row.getHeader().getFields();
    for (int i = 0; i < fields.size(); i++) {
      if (row.getValue(i) == null) {
        continue;
      }
      values[fieldIndices.get(fields.get(i))] = row.getValue(i);
    }
  }

  private RowStream executeIntersectJoin(Join join, Table tableA, Table tableB)
      throws PhysicalException {
    Header headerA = tableA.getHeader();
    Header headerB = tableB.getHeader();
    List<Field> newFields = new ArrayList<>();
    Map<Field, Integer> fieldIndices = new HashMap<>();
    for (Field field : headerA.getFields()) {
      if (fieldIndices.containsKey(field)) {
        continue;
      }
      fieldIndices.put(field, newFields.size());
      newFields.add(field);
    }
    for (Field field : headerB.getFields()) {
      if (fieldIndices.containsKey(field)) {
        continue;
      }
      fieldIndices.put(field, newFields.size());
      newFields.add(field);
    }

    // 目前只支持使用时间戳和顺序
    if (join.getJoinBy().equals(Constants.KEY)) {
      // 检查时间戳
      if (!headerA.hasKey() || !headerB.hasKey()) {
        throw new InvalidOperatorParameterException(
            "row streams for join operator by time should have timestamp.");
      }
      Header newHeader = new Header(Field.KEY, newFields);
      List<Row> newRows = new ArrayList<>();

      int index1 = 0, index2 = 0;
      while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
        Row rowA = tableA.getRow(index1), rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        long timestamp;
        if (rowA.getKey() == rowB.getKey()) {
          timestamp = rowA.getKey();
          writeToNewRow(values, rowA, fieldIndices);
          writeToNewRow(values, rowB, fieldIndices);
          index1++;
          index2++;
        } else if (rowA.getKey() < rowB.getKey()) {
          timestamp = rowA.getKey();
          writeToNewRow(values, rowA, fieldIndices);
          index1++;
        } else {
          timestamp = rowB.getKey();
          writeToNewRow(values, rowB, fieldIndices);
          index2++;
        }
        newRows.add(new Row(newHeader, timestamp, values));
      }

      for (; index1 < tableA.getRowSize(); index1++) {
        Row rowA = tableA.getRow(index1);
        Object[] values = new Object[newHeader.getFieldSize()];
        writeToNewRow(values, rowA, fieldIndices);
        newRows.add(new Row(newHeader, rowA.getKey(), values));
      }

      for (; index2 < tableB.getRowSize(); index2++) {
        Row rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        writeToNewRow(values, rowB, fieldIndices);
        newRows.add(new Row(newHeader, rowB.getKey(), values));
      }
      return new Table(newHeader, newRows);
    } else if (join.getJoinBy().equals(Constants.ORDINAL)) {
      if (headerA.hasKey() || headerB.hasKey()) {
        throw new InvalidOperatorParameterException(
            "row streams for join operator by ordinal shouldn't have timestamp.");
      }
      Header newHeader = new Header(newFields);
      List<Row> newRows = new ArrayList<>();

      int index1 = 0, index2 = 0;
      while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
        Row rowA = tableA.getRow(index1), rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        writeToNewRow(values, rowA, fieldIndices);
        writeToNewRow(values, rowB, fieldIndices);
        index1++;
        index2++;
        newRows.add(new Row(newHeader, values));
      }
      for (; index1 < tableA.getRowSize(); index1++) {
        Row rowA = tableA.getRow(index1);
        Object[] values = new Object[newHeader.getFieldSize()];
        writeToNewRow(values, rowA, fieldIndices);
        newRows.add(new Row(newHeader, values));
      }

      for (; index2 < tableB.getRowSize(); index2++) {
        Row rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        writeToNewRow(values, rowB, fieldIndices);
        newRows.add(new Row(newHeader, values));
      }
      return new Table(newHeader, newRows);
    } else {
      throw new InvalidOperatorParameterException(
          "join operator is not support for field "
              + join.getJoinBy()
              + " except for "
              + Constants.KEY
              + " and "
              + Constants.ORDINAL);
    }
  }

  private RowStream executePathUnion(PathUnion union, Table tableA, Table tableB)
      throws PhysicalException {
    // 检查时间是否一致
    Header headerA = tableA.getHeader();
    Header headerB = tableB.getHeader();
    if (headerA.hasKey() ^ headerB.hasKey()) {
      throw new InvalidOperatorParameterException("row stream to be union must have same fields");
    }
    boolean hasKey = headerA.hasKey();
    Set<Field> targetFieldSet = new HashSet<>();
    targetFieldSet.addAll(headerA.getFields());
    targetFieldSet.addAll(headerB.getFields());
    List<Field> targetFields = new ArrayList<>(targetFieldSet);
    Header targetHeader;
    List<Row> rows = new ArrayList<>();
    if (!hasKey) {
      targetHeader = new Header(targetFields);
      for (Row row : tableA.getRows()) {
        rows.add(RowUtils.transform(row, targetHeader));
      }
      for (Row row : tableB.getRows()) {
        rows.add(RowUtils.transform(row, targetHeader));
      }
    } else {
      targetHeader = new Header(Field.KEY, targetFields);
      int index1 = 0, index2 = 0;
      while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
        Row row1 = tableA.getRow(index1);
        Row row2 = tableB.getRow(index2);
        if (row1.getKey() <= row2.getKey()) {
          rows.add(RowUtils.transform(row1, targetHeader));
          index1++;
        } else {
          rows.add(RowUtils.transform(row2, targetHeader));
          index2++;
        }
      }
      for (; index1 < tableA.getRowSize(); index1++) {
        rows.add(RowUtils.transform(tableA.getRow(index1), targetHeader));
      }
      for (; index2 < tableB.getRowSize(); index2++) {
        rows.add(RowUtils.transform(tableB.getRow(index2), targetHeader));
      }
    }
    return new Table(targetHeader, rows);
  }

  private RowStream executeUnion(Union union, Table tableA, Table tableB) throws PhysicalException {
    // 将左右两表的列Reorder
    Reorder reorderA = new Reorder(EmptySource.EMPTY_SOURCE, union.getLeftOrder());
    Reorder reorderB = new Reorder(EmptySource.EMPTY_SOURCE, union.getRightOrder());
    tableA = transformToTable(executeReorder(reorderA, tableA));
    tableB = transformToTable(executeReorder(reorderB, tableB));

    // 检查输入两表的header是否可比较
    checkHeadersComparable(tableA.getHeader(), tableB.getHeader());

    // 判断是否去重
    if (union.isDistinct()) {
      return executeUnionDistinct(tableA, tableB);
    } else {
      return executeUnionAll(tableA, tableB);
    }
  }

  private RowStream executeUnionAll(Table tableA, Table tableB) {
    boolean hasKey = tableA.getHeader().hasKey();
    Header targetHeader = tableA.getHeader();
    List<Row> targetRows = tableA.getRows();
    for (Row rowB : tableB.getRows()) {
      if (hasKey) {
        targetRows.add(new Row(targetHeader, rowB.getKey(), rowB.getValues()));
      } else {
        targetRows.add(new Row(targetHeader, rowB.getValues()));
      }
    }
    return new Table(targetHeader, targetRows);
  }

  private RowStream executeUnionDistinct(Table tableA, Table tableB) throws PhysicalException {
    boolean hasKey = tableA.getHeader().hasKey();
    Header targetHeader = tableA.getHeader();

    if (tableA.getHeader().getFields().isEmpty() || tableB.getHeader().getFields().isEmpty()) {
      throw new InvalidOperatorParameterException(
          "row stream to be union must have non-empty fields");
    }

    // 检查是否需要类型转换
    boolean needTypeCast =
        checkNeedTypeCast(
            tableA.getRows(),
            tableB.getRows(),
            tableA.getHeader().getField(0).getName(),
            tableB.getHeader().getField(0).getName());

    int hash;
    List<Row> targetRows = new ArrayList<>();
    HashMap<Integer, List<Row>> hashMap = new HashMap<>();
    // 扫描左表建立哈希表
    tableAScan:
    for (Row rowA : tableA.getRows()) {
      if (hasKey) {
        hash = Objects.hash(rowA.getKey());
      } else {
        Value value = rowA.getAsValue(0);
        if (value == null) {
          continue;
        }
        hash = getHash(value, needTypeCast);
      }
      List<Row> rowsExist = hashMap.computeIfAbsent(hash, k -> new ArrayList<>());
      // 去重
      for (Row rowExist : rowsExist) {
        if (isValueEqualRow(rowExist, rowA, hasKey)) {
          continue tableAScan;
        }
      }
      rowsExist.add(rowA);
      targetRows.add(rowA);
    }

    // 扫描右表
    tableBScan:
    for (Row rowB : tableB.getRows()) {
      if (hasKey) {
        hash = Objects.hash(rowB.getKey());
      } else {
        Value value = rowB.getAsValue(0);
        if (value == null) {
          continue;
        }
        hash = getHash(value, needTypeCast);
      }

      // 去重
      List<Row> rowsExist = hashMap.computeIfAbsent(hash, k -> new ArrayList<>());
      for (Row rowExist : rowsExist) {
        if (isValueEqualRow(rowExist, rowB, hasKey)) {
          continue tableBScan;
        }
      }

      Row row =
          hasKey
              ? new Row(targetHeader, rowB.getKey(), rowB.getValues())
              : new Row(targetHeader, rowB.getValues());
      rowsExist.add(row);
      targetRows.add(row);
    }

    return new Table(targetHeader, targetRows);
  }

  private RowStream executeExcept(Except except, Table tableA, Table tableB)
      throws PhysicalException {
    // 将左右两表的列Reorder
    Reorder reorderA = new Reorder(EmptySource.EMPTY_SOURCE, except.getLeftOrder());
    Reorder reorderB = new Reorder(EmptySource.EMPTY_SOURCE, except.getRightOrder());
    tableA = transformToTable(executeReorder(reorderA, tableA));
    tableB = transformToTable(executeReorder(reorderB, tableB));

    // 检查输入两表的header是否可比较
    checkHeadersComparable(tableA.getHeader(), tableB.getHeader());

    if (tableA.getHeader().getFields().isEmpty() || tableB.getHeader().getFields().isEmpty()) {
      throw new InvalidOperatorParameterException(
          "row stream to be except must have non-empty fields");
    }

    // 检查是否需要类型转换
    boolean needTypeCast =
        checkNeedTypeCast(
            tableA.getRows(),
            tableB.getRows(),
            tableA.getHeader().getField(0).getName(),
            tableB.getHeader().getField(0).getName());

    boolean isDistinct = except.isDistinct();
    boolean hasKey = tableA.getHeader().hasKey();
    int hash;
    List<Row> targetRows = new ArrayList<>();
    HashMap<Integer, List<Row>> res = new HashMap<>();
    HashMap<Integer, List<Row>> rowsBMap = new HashMap<>();

    // 扫描右表建立哈希表
    for (Row rowB : tableB.getRows()) {
      if (hasKey) {
        hash = Objects.hash(rowB.getKey());
      } else {
        Value value = rowB.getAsValue(0);
        if (value == null) {
          continue;
        }
        hash = getHash(value, needTypeCast);
      }
      List<Row> rowsBExist = rowsBMap.computeIfAbsent(hash, k -> new ArrayList<>());
      rowsBExist.add(rowB);
    }

    // 扫描左表
    tableAScan:
    for (Row rowA : tableA.getRows()) {
      if (hasKey) {
        hash = Objects.hash(rowA.getKey());
      } else {
        Value value = rowA.getAsValue(0);
        if (value == null) {
          continue;
        }
        hash = getHash(value, needTypeCast);
      }

      // 筛去左表和右表的公共部分
      List<Row> rowsB = rowsBMap.computeIfAbsent(hash, k -> new ArrayList<>());
      for (Row rowB : rowsB) {
        if (isValueEqualRow(rowA, rowB, hasKey)) {
          continue tableAScan;
        }
      }

      // 去重
      if (isDistinct) {
        List<Row> rowsExist = res.computeIfAbsent(hash, k -> new ArrayList<>());
        for (Row rowExist : rowsExist) {
          if (isValueEqualRow(rowA, rowExist, hasKey)) {
            continue tableAScan;
          }
        }
        rowsExist.add(rowA);
      }
      targetRows.add(rowA);
    }

    Header targetHeader = tableA.getHeader();
    return new Table(targetHeader, targetRows);
  }

  private RowStream executeIntersect(Intersect intersect, Table tableA, Table tableB)
      throws PhysicalException {
    // 将左右两表的列reorder
    Reorder reorderA = new Reorder(EmptySource.EMPTY_SOURCE, intersect.getLeftOrder());
    Reorder reorderB = new Reorder(EmptySource.EMPTY_SOURCE, intersect.getRightOrder());
    tableA = transformToTable(executeReorder(reorderA, tableA));
    tableB = transformToTable(executeReorder(reorderB, tableB));

    // 检查输入两表的header是否可比较
    checkHeadersComparable(tableA.getHeader(), tableB.getHeader());

    if (tableA.getHeader().getFields().isEmpty() || tableB.getHeader().getFields().isEmpty()) {
      throw new InvalidOperatorParameterException(
          "row stream to be intersect must have non-empty fields");
    }

    // 检查是否需要类型转换
    boolean needTypeCast =
        checkNeedTypeCast(
            tableA.getRows(),
            tableB.getRows(),
            tableA.getHeader().getField(0).getName(),
            tableB.getHeader().getField(0).getName());

    boolean isDistinct = intersect.isDistinct();
    boolean hasKey = tableA.getHeader().hasKey();
    int hash;
    List<Row> targetRows = new ArrayList<>();
    HashMap<Integer, List<Row>> ret = new HashMap<>();
    HashMap<Integer, List<Row>> rowsBMap = new HashMap<>();

    // 扫描右表建立哈希表
    for (Row rowB : tableB.getRows()) {
      if (hasKey) {
        hash = Objects.hash(rowB.getKey());
      } else {
        Value value = rowB.getAsValue(0);
        if (value == null) {
          continue;
        }
        hash = getHash(value, needTypeCast);
      }
      List<Row> rowsBExist = rowsBMap.computeIfAbsent(hash, k -> new ArrayList<>());
      rowsBExist.add(rowB);
    }

    // 扫描左表
    tableAScan:
    for (Row rowA : tableA.getRows()) {
      if (hasKey) {
        hash = Objects.hash(rowA.getKey());
      } else {
        Value value = rowA.getAsValue(0);
        if (value == null) {
          continue;
        }
        hash = getHash(value, needTypeCast);
      }
      List<Row> rowsB = rowsBMap.computeIfAbsent(hash, k -> new ArrayList<>());
      List<Row> rowsExist = ret.computeIfAbsent(hash, k -> new ArrayList<>());

      // 去重
      if (isDistinct) {
        for (Row rowExist : rowsExist) {
          if (isValueEqualRow(rowA, rowExist, hasKey)) {
            continue tableAScan;
          }
        }
      }

      // 保留左表和右表的公共部分
      for (Row rowB : rowsB) {
        if (isValueEqualRow(rowA, rowB, hasKey)) {
          rowsExist.add(rowA);
          targetRows.add(rowA);
          continue tableAScan;
        }
      }
    }

    Header targetHeader = tableA.getHeader();
    return new Table(targetHeader, targetRows);
  }

  private static class NaiveOperatorMemoryExecutorHolder {

    private static final NaiveOperatorMemoryExecutor INSTANCE = new NaiveOperatorMemoryExecutor();

    private NaiveOperatorMemoryExecutorHolder() {}
  }

  private static class EmptySource implements Source {

    public static final EmptySource EMPTY_SOURCE = new EmptySource();

    @Override
    public SourceType getType() {
      return null;
    }

    @Override
    public Source copy() {
      return null;
    }
  }
}
