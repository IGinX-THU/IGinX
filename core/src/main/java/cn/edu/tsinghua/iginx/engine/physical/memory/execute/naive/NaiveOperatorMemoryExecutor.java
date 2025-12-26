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
import static cn.edu.tsinghua.iginx.engine.shared.Constants.*;
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
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.expr.KeyExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Min;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSequence;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Distinct;
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
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Union;
import cn.edu.tsinghua.iginx.engine.shared.operator.ValueToSelectedPath;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.ConstantSource;
import cn.edu.tsinghua.iginx.engine.shared.source.EmptySource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NaiveOperatorMemoryExecutor implements OperatorMemoryExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NaiveOperatorMemoryExecutor.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private NaiveOperatorMemoryExecutor() {}

  public static NaiveOperatorMemoryExecutor getInstance() {
    return NaiveOperatorMemoryExecutorHolder.INSTANCE;
  }

  @Override
  public RowStream executeUnaryOperator(
      UnaryOperator operator, RowStream stream, RequestContext context) throws PhysicalException {
    Table table = transformToTable(stream);
    table.setContext(context);
    switch (operator.getType()) {
      case Project:
        return executeProject((Project) operator, table);
      case Select:
        return executeSelect((Select) operator, table);
      case Sort:
        return executeSort((Sort) operator, table);
      case Limit:
        return executeLimit((Limit) operator, table);
      case Downsample:
        return executeDownsample((Downsample) operator, table);
      case RowTransform:
        return executeRowTransform((RowTransform) operator, table);
      case SetTransform:
        return executeSetTransform((SetTransform) operator, table);
      case MappingTransform:
        return executeMappingTransform((MappingTransform) operator, table);
      case Rename:
        return executeRename((Rename) operator, table);
      case Reorder:
        return executeReorder((Reorder) operator, table);
      case AddSchemaPrefix:
        return executeAddSchemaPrefix((AddSchemaPrefix) operator, table);
      case GroupBy:
        return executeGroupBy((GroupBy) operator, table);
      case AddSequence:
        return executeAddSequence((AddSequence) operator, table);
      case RemoveNullColumn:
        return executeRemoveNullColumn(table);
      case Distinct:
        return executeDistinct((Distinct) operator, table);
      case ValueToSelectedPath:
        return executeValueToSelectedPath((ValueToSelectedPath) operator, table);
      default:
        throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
    }
  }

  @Override
  public RowStream executeBinaryOperator(
      BinaryOperator operator, RowStream streamA, RowStream streamB, RequestContext context)
      throws PhysicalException {
    Table tableA = transformToTable(streamA);
    Table tableB = transformToTable(streamB);
    tableA.setContext(context);
    tableB.setContext(context);
    switch (operator.getType()) {
      case Join:
        return executeJoin((Join) operator, tableA, tableB);
      case CrossJoin:
        return executeCrossJoin((CrossJoin) operator, tableA, tableB);
      case InnerJoin:
        return executeInnerJoin((InnerJoin) operator, tableA, tableB);
      case OuterJoin:
        return executeOuterJoin((OuterJoin) operator, tableA, tableB);
      case SingleJoin:
        return executeSingleJoin((SingleJoin) operator, tableA, tableB);
      case MarkJoin:
        return executeMarkJoin((MarkJoin) operator, tableA, tableB);
      case PathUnion:
        return executePathUnion((PathUnion) operator, tableA, tableB);
      case Union:
        return executeUnion((Union) operator, tableA, tableB);
      case Except:
        return executeExcept((Except) operator, tableA, tableB);
      case Intersect:
        return executeIntersect((Intersect) operator, tableA, tableB);
      default:
        throw new UnexpectedOperatorException("unknown binary operator: " + operator.getType());
    }
  }

  public static Table transformToTable(RowStream stream) throws PhysicalException {
    if (stream instanceof Table) {
      return (Table) stream;
    }
    try (RowStream ignored = stream) {
      Header header = stream.getHeader();
      List<Row> rows = new ArrayList<>();
      while (stream.hasNext()) {
        rows.add(stream.next());
      }
      return new Table(header, rows);
    }
  }

  private RowStream executeProject(Project project, Table table) throws PhysicalException {
    Source source = project.getSource();
    switch (source.getType()) {
      case Operator:
      case Empty:
        return executeProjectFromOperator(project, table);
      case Constant:
        ConstantSource constantSource = (ConstantSource) source;
        return new Table(RowUtils.buildConstRow(constantSource.getExpressionList()));
      default:
        throw new PhysicalException(
            "Unexpected project source type in memory task: " + source.getType());
    }
  }

  private RowStream executeProjectFromOperator(Project project, Table table) {
    Pair<Header, List<Integer>> pair =
        table.getHeader().projectedHeader(project.getPatterns(), project.isRemainKey());
    Header targetHeader = pair.getK();
    List<Integer> indexList = pair.getV();
    List<Field> targetFields = targetHeader.getFields();
    List<Row> targetRows = new ArrayList<>();
    table.reset();
    while (table.hasNext()) {
      Row row = table.next();
      Object[] objects = new Object[targetFields.size()];
      for (int i = 0; i < targetFields.size(); i++) {
        objects[i] = row.getValue(indexList.get(i));
      }
      targetRows.add(new Row(targetHeader, row.getKey(), objects));
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
    RowTransform preRowTransform = HeaderUtils.checkSortHeader(table.getHeader(), sort);
    if (preRowTransform != null) {
      table = transformToTable(executeRowTransform(preRowTransform, table));
    }

    List<Boolean> ascendingList = sort.getAscendingList();
    RowUtils.sortRows(table.getRows(), ascendingList, sort.getSortByCols());
    return table;
  }

  private RowStream executeLimit(Limit limit, Table table) {
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
          "downsample operator is not support for row stream without key.");
    }
    if (downsample.notSetInterval() && table.getRowSize() <= 0) {
      return Table.EMPTY_TABLE_WITH_KEY;
    }

    long precision = downsample.getPrecision();
    Map<List<String>, Table> rowTransformMap = new HashMap<>();
    List<Table> tableList = new ArrayList<>();
    boolean firstCol = true;
    for (FunctionCall functionCall : downsample.getFunctionCallList()) {
      SetMappingFunction function = (SetMappingFunction) functionCall.getFunction();
      FunctionParams params = functionCall.getParams();

      Table functable = RowUtils.preRowTransform(table, rowTransformMap, functionCall);
      Header tmpHeader;
      TreeMap<Long, List<Row>> groups = RowUtils.computeDownsampleGroup(downsample, functable);

      // <<window_start, window_end> row>
      List<Pair<Pair<Long, Long>, Row>> transformedRawRows = new ArrayList<>();
      for (Map.Entry<Long, List<Row>> entry : groups.entrySet()) {
        tmpHeader = functable.getHeader();
        long windowStartKey = entry.getKey();
        long windowEndKey = windowStartKey + precision - 1;
        List<Row> group = entry.getValue();

        if (params.isDistinct()) {
          if (!isCanUseSetQuantifierFunction(function.getIdentifier())) {
            throw new IllegalArgumentException(
                "function " + function.getIdentifier() + " can't use DISTINCT");
          }
          // min和max无需去重
          if (!function.getIdentifier().equals(Max.MAX)
              && !function.getIdentifier().equals(Min.MIN)) {
            try (Table t = RowUtils.project(tmpHeader, group, params.getPaths())) {
              group = removeDuplicateRows(t.getRows());
              tmpHeader = t.getHeader();
            } catch (PhysicalException e) {
              LOGGER.error(
                  "encounter error when execute distinct in set mapping function {}",
                  function.getIdentifier(),
                  e);
            }
          }
        }

        try {
          Row row = function.transform(new Table(tmpHeader, group), params);
          if (row != null) {
            transformedRawRows.add(new Pair<>(new Pair<>(windowStartKey, windowEndKey), row));
          }
        } catch (Exception e) {
          throw new PhysicalTaskExecuteFailureException(
              "encounter error when execute set mapping function " + function.getIdentifier() + ".",
              e);
        }
      }
      if (transformedRawRows.isEmpty()) {
        return Table.EMPTY_TABLE_WITH_KEY;
      }

      // 只让第一张表保留 window_start, window_end 列，这样按key join后无需删除重复列
      List<Field> fields = transformedRawRows.get(0).v.getHeader().getFields();
      if (firstCol) {
        fields.add(0, new Field(WINDOW_START_COL, DataType.LONG));
        fields.add(1, new Field(WINDOW_END_COL, DataType.LONG));
      }
      Header newHeader = new Header(Field.KEY, fields);
      List<Row> transformedRows = new ArrayList<>();
      Object[] values = new Object[transformedRawRows.get(0).v.getValues().length + 2];
      for (Pair<Pair<Long, Long>, Row> pair : transformedRawRows) {
        if (firstCol) {
          values[0] = pair.k.k;
          values[1] = pair.k.v;
          System.arraycopy(pair.v.getValues(), 0, values, 2, pair.v.getValues().length);
          transformedRows.add(new Row(newHeader, pair.k.k, values));
        } else {
          transformedRows.add(new Row(newHeader, pair.k.k, pair.v.getValues()));
        }
        values = new Object[transformedRawRows.get(0).v.getValues().length + 2];
      }
      tableList.add(new Table(newHeader, transformedRows));
      firstCol = false;
    }

    // key = window_start，而每个窗口长度一样，因此多表中key相同的列就是同一个窗口的结果，可以按key join
    return RowUtils.joinMultipleTablesByKey(tableList);
  }

  private RowStream executeRowTransform(RowTransform rowTransform, Table table)
      throws PhysicalException {
    List<FunctionCall> functionCallList = rowTransform.getFunctionCallList();
    return RowUtils.calRowTransform(table, functionCallList);
  }

  private RowStream executeSetTransform(SetTransform setTransform, Table table)
      throws PhysicalException {
    List<FunctionCall> functionList = setTransform.getFunctionCallList();
    Map<List<String>, Table> rowTransformMap = new HashMap<>();
    Map<List<String>, Table> distinctMap = new HashMap<>();
    List<Row> rows = new ArrayList<>();

    for (FunctionCall functionCall : functionList) {
      SetMappingFunction function = (SetMappingFunction) functionCall.getFunction();
      FunctionParams params = functionCall.getParams();
      Table functable = RowUtils.preRowTransform(table, rowTransformMap, functionCall);
      if (setTransform.isDistinct()) {
        // min和max无需去重
        if (!function.getIdentifier().equals(Max.MAX)
            && !function.getIdentifier().equals(Min.MIN)) {
          if (distinctMap.containsKey(params.getPaths())) {
            functable = distinctMap.get(params.getPaths());
          } else {
            Distinct distinct = new Distinct(EmptySource.EMPTY_SOURCE, params.getPaths());
            functable = transformToTable(executeDistinct(distinct, functable));
            distinctMap.put(params.getPaths(), functable);
          }
        }
      }

      try {
        Row row = function.transform(functable, params);
        if (row != null) {
          rows.add(row);
        }
      } catch (Exception e) {
        throw new PhysicalTaskExecuteFailureException(
            "encounter error when execute set mapping function " + function.getIdentifier() + ".",
            e);
      }
    }

    if (rows.isEmpty()) {
      return Table.EMPTY_TABLE;
    }

    Row combinedRow = combineMultipleColumns(rows);
    Header header = combinedRow.getHeader();

    return new Table(header, Collections.singletonList(combinedRow));
  }

  private RowStream executeMappingTransform(MappingTransform mappingTransform, Table table)
      throws PhysicalException {
    List<FunctionCall> functionCallList = mappingTransform.getFunctionCallList();
    return RowUtils.calMappingTransform(table, functionCallList);
  }

  private RowStream executeRename(Rename rename, Table table) throws PhysicalException {
    Header header = table.getHeader();
    List<Pair<String, String>> aliasList = rename.getAliasList();
    Pair<Header, Integer> pair = header.renamedHeader(aliasList, rename.getIgnorePatterns());
    Header newHeader = pair.k;
    int colIndex = pair.v;

    List<Row> rows = new ArrayList<>();
    if (colIndex == -1) {
      table.getRows().forEach(row -> rows.add(new Row(newHeader, row.getKey(), row.getValues())));
    } else {
      HashSet<Long> keySet = new HashSet<>();
      for (Row row : table.getRows()) {
        Row newRow = RowUtils.transformColumnToKey(newHeader, row, colIndex);
        if (keySet.contains(newRow.getKey())) {
          throw new PhysicalTaskExecuteFailureException("duplicated key found: " + newRow.getKey());
        }
        keySet.add(newRow.getKey());
        rows.add(newRow);
      }
    }

    return new Table(newHeader, rows);
  }

  private RowStream executeAddSchemaPrefix(AddSchemaPrefix addSchemaPrefix, Table table) {
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
    RowTransform preRowTransform = HeaderUtils.checkGroupByHeader(table.getHeader(), groupBy);
    if (preRowTransform != null) {
      table = transformToTable(executeRowTransform(preRowTransform, table));
    }

    List<Row> rows = RowUtils.cacheGroupByResult(groupBy, table);
    if (rows.isEmpty()) {
      return Table.EMPTY_TABLE;
    }
    Header header = rows.get(0).getHeader();
    return new Table(header, rows);
  }

  private RowStream executeAddSequence(AddSequence addSequence, Table table) {
    Header header = table.getHeader();
    List<Field> targetFields = new ArrayList<>(header.getFields());
    addSequence.getColumns().forEach(column -> targetFields.add(new Field(column, DataType.LONG)));
    Header newHeader = new Header(header.getKey(), targetFields);

    List<Row> rows = new ArrayList<>();
    int oldSize = header.getFieldSize();
    int newSize = targetFields.size();
    int sequenceSize = newSize - oldSize;
    List<Long> cur = new ArrayList<>(addSequence.getStartList());
    List<Long> increments = new ArrayList<>(addSequence.getIncrementList());
    table
        .getRows()
        .forEach(
            row -> {
              Object[] values = new Object[newSize];
              System.arraycopy(row.getValues(), 0, values, 0, oldSize);
              for (int i = 0; i < sequenceSize; i++) {
                values[oldSize + i] = cur.get(i);
                cur.set(i, cur.get(i) + increments.get(i));
              }
              rows.add(new Row(newHeader, row.getKey(), values));
            });
    return new Table(newHeader, rows);
  }

  private RowStream executeReorder(Reorder reorder, Table table) throws PhysicalException {
    Header header = table.getHeader();

    Header.ReorderedHeaderWrapped res =
        header.reorderedHeaderWrapped(reorder.getPatterns(), reorder.getIsPyUDF());
    Header newHeader = res.getHeader();
    List<Field> targetFields = res.getTargetFields();
    Map<Integer, Integer> reorderMap = res.getReorderMap();

    if (targetFields.isEmpty()) {
      return table.getEmptyTable();
    }

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

  private RowStream executeDistinct(Distinct distinct, Table table) throws PhysicalException {
    Project project = new Project(EmptySource.EMPTY_SOURCE, distinct.getPatterns(), null);
    table = transformToTable(executeProject(project, table));

    if (table.getHeader().getFields().isEmpty()) {
      return table;
    }

    Header newHeader = new Header(table.getHeader().getFields());
    List<Row> targetRows = removeDuplicateRows(table.getRows());

    return new Table(newHeader, targetRows);
  }

  private RowStream executeRemoveNullColumn(Table table) {
    Header header = table.getHeader();
    int fieldSize = header.getFieldSize();
    List<Row> rows = table.getRows();

    List<Integer> remainIndexes = new ArrayList<>();
    for (int i = 0; i < fieldSize; i++) {
      int finalI = i;
      boolean isEmptyColumn = rows.stream().allMatch(row -> row.getValue(finalI) == null);
      if (!isEmptyColumn) {
        remainIndexes.add(finalI);
      }
    }

    int remainColumnSize = remainIndexes.size();
    if (remainColumnSize == fieldSize) { // 没有空列
      return table;
    } else if (remainIndexes.isEmpty()) { // 全是空列
      return rows.isEmpty() ? table : table.getEmptyTable();
    }

    List<Field> newFields = new ArrayList<>(remainColumnSize);
    for (int index : remainIndexes) {
      newFields.add(header.getField(index));
    }
    Header newHeader = new Header(header.getKey(), newFields);

    List<Row> newRows = new ArrayList<>();
    for (Row row : rows) {
      Object[] values = new Object[remainColumnSize];
      for (int i = 0; i < remainColumnSize; i++) {
        values[i] = row.getValue(remainIndexes.get(i));
      }
      newRows.add(new Row(newHeader, row.getKey(), values));
    }
    return new Table(newHeader, newRows);
  }

  private RowStream executeValueToSelectedPath(ValueToSelectedPath operator, Table table) {
    String prefix = operator.getPrefix();
    boolean prefixIsEmpty = prefix.isEmpty();

    int fieldSize = table.getHeader().getFieldSize();
    Header targetHeader =
        new Header(Collections.singletonList(new Field("SelectedPath", DataType.BINARY)));
    List<Row> targetRows = new ArrayList<>();
    table
        .getRows()
        .forEach(
            row -> {
              for (int i = 0; i < fieldSize; i++) {
                String valueStr = row.getAsValue(i).getAsString();
                if (valueStr.isEmpty()) {
                  continue;
                }
                String path = prefixIsEmpty ? valueStr : prefix + DOT + valueStr;
                Object[] value = new Object[1];
                value[0] = path.getBytes(StandardCharsets.UTF_8);
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
      return executeJoinByKey(tableA, tableB, true, true);
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

  private RowStream executeCrossJoin(CrossJoin crossJoin, Table tableA, Table tableB) {
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
    if (innerJoin.isJoinByKey()) {
      Sort sortByKey =
          new Sort(
              EmptySource.EMPTY_SOURCE,
              Collections.singletonList(new KeyExpression(KEY)),
              Collections.singletonList(Sort.SortType.ASC));
      tableA = transformToTable(executeSort(sortByKey, tableA));
      tableB = transformToTable(executeSort(sortByKey, tableB));
      return executeJoinByKey(tableA, tableB, false, false);
    }

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

  private RowStream executeJoinByKey(Table tableA, Table tableB, boolean isLeft, boolean isRight)
      throws PhysicalException {
    Header headerA = tableA.getHeader();
    Header headerB = tableB.getHeader();
    // 检查时间戳
    if (!headerA.hasKey() || !headerB.hasKey()) {
      throw new InvalidOperatorParameterException(
          "row streams for join operator by key should have key.");
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
        index1++;
        if (!isLeft) { // 内连接和右连接不保留该结果
          continue;
        }
        timestamp = rowA.getKey();
        System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
      } else {
        index2++;
        if (!isRight) { // 内连接和左连接不保留该结果
          continue;
        }
        timestamp = rowB.getKey();
        System.arraycopy(
            rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
      }
      newRows.add(new Row(newHeader, timestamp, values));
    }

    // 左连接和全连接才保留该结果
    if (isLeft) {
      for (; index1 < tableA.getRowSize(); index1++) {
        Row rowA = tableA.getRow(index1);
        Object[] values = new Object[newHeader.getFieldSize()];
        System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
        newRows.add(new Row(newHeader, rowA.getKey(), values));
      }
    }

    // 右连接和全连接才保留该结果
    if (isRight) {
      for (; index2 < tableB.getRowSize(); index2++) {
        Row rowB = tableB.getRow(index2);
        Object[] values = new Object[newHeader.getFieldSize()];
        System.arraycopy(
            rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
        newRows.add(new Row(newHeader, rowB.getKey(), values));
      }
    }

    return new Table(newHeader, newRows);
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
      if (value.isNull()) {
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
    if (outerJoin.isJoinByKey()) {
      Sort sortByKey =
          new Sort(
              EmptySource.EMPTY_SOURCE,
              Collections.singletonList(new KeyExpression(KEY)),
              Collections.singletonList(Sort.SortType.ASC));
      tableA = transformToTable(executeSort(sortByKey, tableA));
      tableB = transformToTable(executeSort(sortByKey, tableB));
      boolean isLeft = outerJoin.getOuterJoinType() != OuterJoinType.RIGHT;
      boolean isRight = outerJoin.getOuterJoinType() != OuterJoinType.LEFT;
      return executeJoinByKey(tableA, tableB, isLeft, isRight);
    }

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
      if (value.isNull()) {
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
      if (value.isNull()) {
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
      if (value.isNull()) {
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
      if (value.isNull()) {
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
    boolean containOverlappedKeys = false;
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
          if (!containOverlappedKeys) {
            containOverlappedKeys = true;
          }
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
      Table table = new Table(newHeader, newRows);
      RequestContext context = null;
      if (tableA.getContext() != null) {
        context = tableA.getContext();
      } else if (tableB.getContext() != null) {
        context = tableB.getContext();
      }
      if (context != null && containOverlappedKeys) {
        context.addWarningMessage("The query results contain overlapped keys.");
      }
      table.setContext(context);

      return table;
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
        if (value.isNull()) {
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
        if (value.isNull()) {
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
        if (value.isNull()) {
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
        if (value.isNull()) {
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
        if (value.isNull()) {
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
        if (value.isNull()) {
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
}
