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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import static cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils.isCanUseSetQuantifierFunction;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils.getHash;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Min;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowUtils {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final Logger logger = LoggerFactory.getLogger(RowUtils.class);

  public static final BlockingQueue<ForkJoinPool> poolQueue = new LinkedBlockingQueue<>();

  static {
    for (int i = 0; i < config.getParallelGroupByPoolNum(); i++) {
      poolQueue.add(new ForkJoinPool(config.getParallelGroupByPoolSize()));
    }
  }

  public static Row transform(Row row, Header targetHeader) {
    Object[] values = new Object[targetHeader.getFieldSize()];
    for (int i = 0; i < targetHeader.getFieldSize(); i++) {
      Field field = targetHeader.getField(i);
      values[i] = row.getValue(field);
    }
    Row targetRow;
    if (targetHeader.hasKey()) {
      targetRow = new Row(targetHeader, row.getKey(), values);
    } else {
      targetRow = new Row(targetHeader, values);
    }
    return targetRow;
  }

  public static Row combineMultipleColumns(List<Row> columnList) {
    if (columnList == null || columnList.isEmpty()) {
      return Row.EMPTY_ROW;
    }
    if (columnList.size() == 1) {
      return columnList.get(0);
    }

    List<Field> fields = new ArrayList<>();
    List<Object> valuesCombine = new ArrayList<>();
    for (Row cols : columnList) {
      fields.addAll(cols.getHeader().getFields());
      valuesCombine.addAll(Arrays.asList(cols.getValues()));
    }
    Header newHeader =
        columnList.get(0).getHeader().hasKey() ? new Header(Field.KEY, fields) : new Header(fields);
    return new Row(newHeader, columnList.get(0).getKey(), valuesCombine.toArray());
  }

  public static boolean isEqualRow(Row row1, Row row2, boolean compareKey)
      throws PhysicalException {
    if (!row1.getHeader().equals(row2.getHeader())) {
      return false;
    }
    return isValueEqualRow(row1, row2, compareKey);
  }

  public static boolean isValueEqualRow(Row row1, Row row2, boolean compareKey)
      throws PhysicalException {
    if (compareKey && row1.getKey() != row2.getKey()) {
      return false;
    }
    if (row1.getHeader().getFieldSize() != row2.getHeader().getFieldSize()) {
      return false;
    }

    int size = row1.getHeader().getFieldSize();
    for (int index = 0; index < size; index++) {
      Value value1 = row1.getAsValue(index);
      Value value2 = row2.getAsValue(index);
      boolean v1Null = value1.isNull();
      boolean v2Null = value2.isNull();
      if (v1Null && v2Null) {
        continue;
      } else if (v1Null ^ v2Null) {
        return false;
      }
      if (ValueUtils.compare(value1, value2) != 0) {
        return false;
      }
    }
    return true;
  }

  public static void checkJoinColumns(
      List<String> joinColumns, Header headerA, Header headerB, String prefixA, String prefixB)
      throws InvalidOperatorParameterException {
    for (String joinColumn : joinColumns) {
      if (headerA.indexOf(prefixA + DOT + joinColumn) == -1) {
        throw new InvalidOperatorParameterException(
            "TableA has no path: " + prefixA + DOT + joinColumn);
      }
      if (headerB.indexOf(prefixB + DOT + joinColumn) == -1) {
        throw new InvalidOperatorParameterException(
            "TableB has no path: " + prefixB + DOT + joinColumn);
      }
    }
  }

  public static boolean checkNeedTypeCast(
      List<Row> rowsA, List<Row> rowsB, String joinPathA, String joinPathB) {
    if (!rowsA.isEmpty() && !rowsB.isEmpty()) {
      Value valueA = rowsA.get(0).getAsValue(joinPathA);
      Value valueB = rowsB.get(0).getAsValue(joinPathB);
      if (valueA.getDataType() != valueB.getDataType()) {
        if (ValueUtils.isNumericType(valueA) && ValueUtils.isNumericType(valueB)) {
          return true;
        }
      }
    }
    return false;
  }

  public static Map<Integer, List<Row>> establishHashMap(
      List<Row> rows, String joinPath, boolean needTypeCast) throws PhysicalException {
    Map<Integer, List<Row>> result;

    String use;
    long startTime = System.currentTimeMillis();
    if (config.isEnableParallelOperator()
        && rows.size() > config.getParallelGroupByRowsThreshold()) {
      use = "parallel";
      result = parallelEstablishHashMap(rows, joinPath, needTypeCast);
    } else {
      use = "sequence";
      result = seqEstablishHashMap(rows, joinPath, needTypeCast);
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        String.format(
            "join use %s build, row size: %s, cost time: %s",
            use, rows.size(), endTime - startTime));
    return result;
  }

  private static Map<Integer, List<Row>> seqEstablishHashMap(
      List<Row> rows, String joinPath, boolean needTypeCast) {
    Map<Integer, List<Row>> map = new HashMap<>();
    for (Row row : rows) {
      Value value = row.getAsValue(joinPath);
      if (value == null) {
        continue;
      }
      int hash = getHash(value, needTypeCast);

      List<Row> l = map.computeIfAbsent(hash, k -> new ArrayList<>());
      l.add(row);
    }
    return map;
  }

  private static Map<Integer, List<Row>> parallelEstablishHashMap(
      List<Row> rows, String joinPath, boolean needTypeCast) throws PhysicalException {
    ForkJoinPool pool = null;
    try {
      pool = poolQueue.take();

      int size = config.getParallelGroupByPoolSize();
      int range = rows.size() / size;
      List<Map<Integer, List<Row>>> listOfMaps = new ArrayList<>();

      CountDownLatch latch = new CountDownLatch(size);
      for (int i = 0; i < size; i++) {
        int finalI = i;
        listOfMaps.add(new HashMap<>());
        pool.submit(
            () -> {
              for (int j = finalI * range; j < Math.min((finalI + 1) * range, rows.size()); j++) {
                Row row = rows.get(j);
                Value value = row.getAsValue(joinPath);
                if (value == null) {
                  continue;
                }
                int hash = getHash(value, needTypeCast);

                List<Row> l = listOfMaps.get(finalI).computeIfAbsent(hash, k -> new ArrayList<>());
                l.add(row);
              }
              latch.countDown();
            });
      }
      latch.await();
      Map<Integer, List<Row>> mergedMap =
          listOfMaps
              .parallelStream()
              .flatMap(map -> map.entrySet().stream()) // 将每个Map转换为流
              .collect(
                  Collectors.toConcurrentMap(
                      Map.Entry::getKey, // 键
                      Map.Entry::getValue, // 值
                      (list1, list2) -> { // 合并函数，用于处理重复键
                        list1.addAll(list2); // 合并两个列表
                        return list1;
                      }));
      return mergedMap;
      //      Map<Integer, List<Row>> map =
      //          pool.submit(
      //                  () ->
      //                      Collections.synchronizedList(rows)
      //                          .parallelStream()
      //                          .collect(
      //                              Collectors.groupingBy(
      //                                  row -> {
      //                                    Value value = row.getAsValue(joinPath);
      //                                    if (value == null) {
      //                                      return 0; // nullable value hashcode
      //                                    }
      //                                    return getHash(value, needTypeCast);
      //                                  })))
      //              .get();
      //      return map;
    } catch (InterruptedException e) {
      throw new PhysicalException("parallel build failed");
    } finally {
      if (pool != null) {
        poolQueue.add(pool);
      }
    }
  }

  /**
   * @return <tt>-1</tt>: not sorted <tt>0</tt>: all rows equal <tt>1</tt>: ascending sorted
   *     <tt>2</tt>: descending sorted
   */
  public static int checkRowsSortedByColumns(List<Row> rows, String prefix, List<String> columns)
      throws PhysicalException {
    int res = 0;
    int index = 0;
    while (index < rows.size() - 1) {
      int mark =
          compareRowsSortedByColumns(
              rows.get(index), rows.get(index + 1), prefix, prefix, columns, columns);
      if (mark == -1) {
        if (res == 0) {
          res = 1;
        } else if (res == 2) {
          return -1;
        }
      } else if (mark == 1) {
        if (res == 0) {
          res = 2;
        } else if (res == 1) {
          return -1;
        }
      }
      index++;
    }
    return res;
  }

  /** @return <tt>-1</tt>: row1 < row2 <tt>0</tt>: row1 = row2 <tt>1</tt>: row1 > row2 */
  public static int compareRowsSortedByColumns(
      Row row1,
      Row row2,
      String prefix1,
      String prefix2,
      List<String> columns1,
      List<String> columns2)
      throws PhysicalException {
    assert columns1.size() == columns2.size();
    int size = columns1.size();
    for (int index = 0; index < size; index++) {
      Object value1 = row1.getValue(prefix1 + '.' + columns1.get(index));
      Object value2 = row2.getValue(prefix2 + '.' + columns2.get(index));
      if (value1 == null && value2 == null) {
        return 0;
      } else if (value1 == null) {
        return -1;
      } else if (value2 == null) {
        return 1;
      }
      DataType dataType1 =
          row1.getField(row1.getHeader().indexOf(prefix1 + '.' + columns1.get(index))).getType();
      DataType dataType2 =
          row2.getField(row2.getHeader().indexOf(prefix2 + '.' + columns2.get(index))).getType();
      int cmp = ValueUtils.compare(value1, value2, dataType1, dataType2);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  public static Row constructUnmatchedRow(
      Header header, Row halfRow, String prefix, int anotherRowSize, boolean putLeft) {

    int size = halfRow.getValues().length + anotherRowSize;
    if (halfRow.getHeader().hasKey() && prefix != null) {
      size++;
    }
    Object[] valuesJoin = new Object[size];

    if (putLeft) {
      if (halfRow.getHeader().hasKey() && prefix != null) {
        valuesJoin[0] = halfRow.getKey();
        System.arraycopy(halfRow.getValues(), 0, valuesJoin, 1, halfRow.getValues().length);
      } else {
        System.arraycopy(halfRow.getValues(), 0, valuesJoin, 0, halfRow.getValues().length);
      }
    } else {
      if (halfRow.getHeader().hasKey()) {
        valuesJoin[anotherRowSize] = halfRow.getKey();
        System.arraycopy(
            halfRow.getValues(), 0, valuesJoin, anotherRowSize + 1, halfRow.getValues().length);
      } else {
        System.arraycopy(
            halfRow.getValues(), 0, valuesJoin, anotherRowSize, halfRow.getValues().length);
      }
    }
    return new Row(header, valuesJoin);
  }

  public static Row constructNewRow(Header header, Row rowA, Row rowB, boolean remainKeyA) {
    return constructNewRow(header, rowA, rowB, remainKeyA, Collections.emptyList());
  }

  public static Row constructNewRow(
      Header header, Row rowA, Row rowB, boolean remainKeyA, List<String> extraJoinPaths) {
    Object[] valuesA = rowA.getValues();
    Object[] valuesB = rowB.getValues();

    int size = valuesA.length + valuesB.length;
    int rowAStartIndex = 0, rowBStartIndex = valuesA.length;

    Object[] valuesJoin = new Object[size];

    System.arraycopy(valuesA, 0, valuesJoin, rowAStartIndex, valuesA.length);
    int currentIndex = rowBStartIndex;
    for (int i = 0; i < valuesB.length; i++) {
      String path = rowB.getHeader().getField(i).getName();
      if (!extraJoinPaths.contains(path)) {
        valuesJoin[currentIndex] = valuesB[i];
        currentIndex++;
      }
    }
    return remainKeyA && rowA.getHeader().hasKey()
        ? new Row(header, rowA.getKey(), valuesJoin)
        : new Row(header, valuesJoin);
  }

  public static Row constructNewRow(
      Header header, Row rowA, Row rowB, String prefixA, String prefixB) {
    Object[] valuesA = rowA.getValues();
    Object[] valuesB = rowB.getValues();

    int size = valuesA.length + valuesB.length;
    int rowAStartIndex = 0, rowBStartIndex = valuesA.length;
    if (rowA.getHeader().hasKey() && prefixA != null) {
      size++;
      rowAStartIndex++;
      rowBStartIndex++;
    }
    if (rowB.getHeader().hasKey() && prefixB != null) {
      size++;
      rowBStartIndex++;
    }

    Object[] valuesJoin = new Object[size];

    if (rowA.getHeader().hasKey()) {
      valuesJoin[0] = rowA.getKey();
    }
    if (rowB.getHeader().hasKey()) {
      valuesJoin[rowBStartIndex - 1] = rowB.getKey();
    }
    System.arraycopy(valuesA, 0, valuesJoin, rowAStartIndex, valuesA.length);
    System.arraycopy(valuesB, 0, valuesJoin, rowBStartIndex, valuesB.length);
    return new Row(header, valuesJoin);
  }

  public static Row constructNewRow(
      Header header,
      Row rowA,
      Row rowB,
      String prefixA,
      String prefixB,
      boolean cutRight,
      List<String> joinColumns,
      List<String> extraJoinPaths) {
    Object[] valuesA = rowA.getValues();
    Object[] valuesB = rowB.getValues();

    int size = valuesA.length + valuesB.length - joinColumns.size() - extraJoinPaths.size();
    int rowAStartIndex = 0;
    int rowBStartIndex =
        cutRight ? valuesA.length : valuesA.length - joinColumns.size() - extraJoinPaths.size();
    if (rowA.getHeader().hasKey() && prefixA != null) {
      size++;
      rowAStartIndex++;
      rowBStartIndex++;
    }
    if (rowB.getHeader().hasKey() && prefixB != null) {
      size++;
      rowBStartIndex++;
    }

    Object[] valuesJoin = new Object[size];
    List<String> joinPathB = new ArrayList<>();
    List<String> joinPathA = new ArrayList<>();
    joinColumns.forEach(
        joinColumn -> {
          joinPathA.add(prefixA + DOT + joinColumn);
          joinPathB.add(prefixB + DOT + joinColumn);
        });

    if (rowA.getHeader().hasKey() && prefixA != null) {
      valuesJoin[0] = rowA.getKey();
    }
    if (rowB.getHeader().hasKey() && prefixB != null) {
      valuesJoin[rowBStartIndex - 1] = rowB.getKey();
    }

    if (cutRight) {
      System.arraycopy(valuesA, 0, valuesJoin, rowAStartIndex, valuesA.length);
      int currentIndex = rowBStartIndex;
      for (int i = 0; i < valuesB.length; i++) {
        String path = rowB.getHeader().getField(i).getName();
        if (extraJoinPaths.contains(path)) {
          continue;
        } else if (joinPathB.contains(path)) {
          continue;
        }
        valuesJoin[currentIndex] = valuesB[i];
        currentIndex++;
      }
    } else {
      int currentIndex = rowAStartIndex;
      for (int i = 0; i < valuesA.length; i++) {
        String path = rowA.getHeader().getField(i).getName();
        if (extraJoinPaths.contains(path)) {
          continue;
        } else if (joinPathA.contains(path)) {
          continue;
        }
        valuesJoin[currentIndex] = valuesA[i];
        currentIndex++;
      }
      System.arraycopy(valuesB, 0, valuesJoin, rowBStartIndex, valuesB.length);
    }

    return new Row(header, valuesJoin);
  }

  public static Row constructNewRow(
      Header header, Row rowA, Row rowB, int[] indexOfJoinColumnsInTable, boolean cutRight) {
    Object[] valuesA = rowA.getValues();
    Object[] valuesB = rowB.getValues();

    int size = valuesA.length + valuesB.length - indexOfJoinColumnsInTable.length;
    int rowAStartIndex = 0, rowBStartIndex;
    if (cutRight) {
      rowBStartIndex = valuesA.length;
    } else {
      rowBStartIndex = valuesA.length - indexOfJoinColumnsInTable.length;
    }

    if (rowA.getHeader().hasKey()) {
      size++;
      rowAStartIndex++;
      rowBStartIndex++;
    }
    if (rowB.getHeader().hasKey()) {
      size++;
      rowBStartIndex++;
    }

    Object[] valuesJoin = new Object[size];

    if (rowA.getHeader().hasKey()) {
      valuesJoin[0] = rowA.getKey();
    }
    if (rowB.getHeader().hasKey()) {
      valuesJoin[rowBStartIndex - 1] = rowB.getKey();
    }
    if (cutRight) {
      System.arraycopy(valuesA, 0, valuesJoin, rowAStartIndex, valuesA.length);
      int k = rowBStartIndex;
      flag:
      for (int i = 0; i < valuesB.length; i++) {
        for (int index : indexOfJoinColumnsInTable) {
          if (i == index) {
            continue flag;
          }
        }
        valuesJoin[k++] = valuesB[i];
      }
    } else {
      System.arraycopy(valuesB, 0, valuesJoin, rowBStartIndex, valuesB.length);
      int k = rowAStartIndex;
      flag:
      for (int i = 0; i < valuesA.length; i++) {
        for (int index : indexOfJoinColumnsInTable) {
          if (i == index) {
            continue flag;
          }
        }
        valuesJoin[k++] = valuesA[i];
      }
    }

    return new Row(header, valuesJoin);
  }

  public static Row constructNewRowWithMark(Header header, Row row, boolean markValue) {
    Object[] values = row.getValues();
    Object[] newValues = new Object[values.length + 1];
    System.arraycopy(values, 0, newValues, 0, values.length);
    newValues[values.length] = markValue;
    return row.getHeader().hasKey()
        ? new Row(header, row.getKey(), newValues)
        : new Row(header, newValues);
  }

  public static void fillNaturalJoinColumns(
      List<String> joinColumns, Header headerA, Header headerB, String prefixA, String prefixB)
      throws PhysicalException {
    if (!joinColumns.isEmpty()) {
      throw new InvalidOperatorParameterException(
          "natural inner join operator should not have using operator");
    }
    for (Field fieldA : headerA.getFields()) {
      for (Field fieldB : headerB.getFields()) {
        String joinColumnA = fieldA.getName().replaceFirst(prefixA + '.', "");
        String joinColumnB = fieldB.getName().replaceFirst(prefixB + '.', "");
        if (joinColumnA.equals(joinColumnB)) {
          joinColumns.add(joinColumnA);
        }
      }
    }
    if (joinColumns.isEmpty()) {
      throw new PhysicalException("natural join has no matching columns");
    }
  }

  public static List<String> getSamePathWithSpecificPrefix(
      Header headerA, Header headerB, List<String> prefixList) {
    List<String> res = new ArrayList<>();
    headerA
        .getFields()
        .forEach(
            fieldA -> {
              if (headerB.indexOf(fieldA.getName()) != -1) {
                for (String prefix : prefixList) {
                  if (prefix.endsWith(Constants.ALL_PATH_SUFFIX)) {
                    prefix = prefix.substring(0, prefix.length() - 2);
                  }
                  if (fieldA.getName().startsWith(prefix)) {
                    res.add(fieldA.getName());
                  }
                }
              }
            });
    return res;
  }

  public static boolean equalOnSpecificPaths(Row rowA, Row rowB, List<String> pathList)
      throws PhysicalException {
    for (String path : pathList) {
      Value valueA = rowA.getAsValue(path);
      Value valueB = rowB.getAsValue(path);
      if (ValueUtils.compare(valueA, valueB) != 0) {
        return false;
      }
    }
    return true;
  }

  public static boolean equalOnSpecificPaths(
      Row rowA, Row rowB, String prefixA, String prefixB, List<String> joinColumns)
      throws PhysicalException {
    if (joinColumns == null) {
      joinColumns = new ArrayList<>();
    }
    for (String joinColumn : joinColumns) {
      Value valueA = rowA.getAsValue(prefixA + DOT + joinColumn);
      Value valueB = rowB.getAsValue(prefixB + DOT + joinColumn);
      if (ValueUtils.compare(valueA, valueB) != 0) {
        return false;
      }
    }
    return true;
  }

  public static List<Row> cacheGroupByResult(GroupBy groupBy, Table table)
      throws PhysicalException {
    List<String> cols = groupBy.getGroupByCols();
    int[] colIndex = new int[cols.size()];
    List<Field> fields = new ArrayList<>();
    Header header = table.getHeader();

    int cur = 0;
    for (String col : cols) {
      int index = header.indexOf(col);
      if (index == -1) {
        throw new PhysicalTaskExecuteFailureException(
            String.format("Group by col [%s] not exist.", col));
      }
      colIndex[cur++] = index;
      fields.add(header.getField(index));
    }

    Map<GroupByKey, List<Row>> groups;
    String use;
    long startTime = System.currentTimeMillis();
    if (config.isEnableParallelOperator()
        && table.getRowSize() > config.getParallelGroupByRowsThreshold()) {
      use = "parallel";
      groups = parallelBuild(table, colIndex);
    } else {
      use = "sequence";
      groups = seqBuild(table, colIndex);
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        String.format(
            "groupBy use %s build, row size: %s, cost time: %s",
            use, table.getRowSize(), endTime - startTime));

    return applyFunc(groupBy, fields, header, groups);
  }

  public static List<Row> applyFunc(
      GroupBy groupBy, List<Field> fields, Header header, Map<GroupByKey, List<Row>> groups)
      throws PhysicalException {

    String use;
    long startTime = System.currentTimeMillis();
    if (config.isEnableParallelOperator()
        && groups.size() > config.getParallelApplyFuncGroupsThreshold()) {
      use = "parallel";
      parallelApplyFunc(groupBy, fields, header, groups);
    } else {
      use = "sequence";
      seqApplyFunc(groupBy, fields, header, groups);
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        String.format(
            "groupBy use %s apply, row size: %s, cost time: %s",
            use, groups.size(), endTime - startTime));

    Header newHeader = new Header(fields);
    int fieldSize = newHeader.getFieldSize();

    List<Row> cache = new ArrayList<>();
    for (GroupByKey key : groups.keySet()) {
      Object[] values = new Object[fieldSize];
      for (int i = 0; i < key.getRowValues().size(); i++) {
        Object val = key.getRowValues().get(i);
        if (val instanceof String) {
          values[i] = ((String) val).getBytes();
        } else {
          values[i] = val;
        }
      }
      cache.add(new Row(newHeader, values));
    }
    return cache;
  }

  public static void seqApplyFunc(
      GroupBy groupBy, List<Field> fields, Header header, Map<GroupByKey, List<Row>> groups)
      throws PhysicalException {
    List<FunctionCall> functionCallList = groupBy.getFunctionCallList();
    for (FunctionCall functionCall : functionCallList) {
      SetMappingFunction function = (SetMappingFunction) functionCall.getFunction();
      FunctionParams params = functionCall.getParams();

      boolean hasAddedFields = false;
      for (Map.Entry<GroupByKey, List<Row>> entry : groups.entrySet()) {
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

        try {
          Row row = function.transform(new Table(header, group), params);
          if (row != null) {
            entry.getKey().getFuncRet().addAll(Arrays.asList(row.getValues()));
            if (!hasAddedFields) {
              fields.addAll(row.getHeader().getFields());
              hasAddedFields = true;
            }
          }
        } catch (Exception e) {
          throw new PhysicalTaskExecuteFailureException(
              "encounter error when execute set mapping function " + function.getIdentifier() + ".",
              e);
        }
      }
    }
  }

  private static void parallelApplyFunc(
      GroupBy groupBy, List<Field> fields, Header header, Map<GroupByKey, List<Row>> groups)
      throws PhysicalException {
    List<FunctionCall> functionCallList = groupBy.getFunctionCallList();

    for (FunctionCall functionCall : functionCallList) {
      CountDownLatch latch = new CountDownLatch(1);
      SetMappingFunction function = (SetMappingFunction) functionCall.getFunction();
      FunctionParams params = functionCall.getParams();

      AtomicBoolean hasAddedFields = new AtomicBoolean(false);
      ForkJoinPool pool = null;
      try {
        // 我们可能需要一种退化情况：获取不到线程池的时候，直接串行执行
        pool = poolQueue.take();
        pool.submit(
            () -> {
              groups
                  .entrySet()
                  .parallelStream()
                  .forEach(
                      entry -> {
                        List<Row> group = entry.getValue();

                        if (params.isDistinct()) {
                          if (!isCanUseSetQuantifierFunction(function.getIdentifier())) {
                            throw new IllegalArgumentException(
                                "function " + function.getIdentifier() + " can't use DISTINCT");
                          }
                          // min和max无需去重
                          if (!function.getIdentifier().equals(Max.MAX)
                              && !function.getIdentifier().equals(Min.MIN)) {
                            try {
                              group = removeDuplicateRows(group);
                            } catch (PhysicalException e) {
                              throw new RuntimeException(e);
                            }
                          }
                        }

                        try {
                          Row row = function.transform(new Table(header, group), params);
                          if (row != null) {
                            entry.getKey().getFuncRet().addAll(Arrays.asList(row.getValues()));
                            if (hasAddedFields.compareAndSet(false, true)) {
                              fields.addAll(row.getHeader().getFields());
                            }
                          }
                        } catch (Exception e) {
                          logger.error("encounter error when execute set mapping function ");
                        }
                      });
              latch.countDown();
            });

        try {
          latch.await();
        } catch (InterruptedException e) {
          throw new PhysicalException("Interrupt when latch await ", e);
        }
      } catch (InterruptedException e) {
        throw new PhysicalException("Interrupt when parallel apply func", e);
      } finally {
        if (pool != null) {
          poolQueue.add(pool);
        }
      }
    }
  }

  private static Map<GroupByKey, List<Row>> seqBuild(Table table, int[] colIndex) {
    Map<GroupByKey, List<Row>> groups = new HashMap<>();
    while (table.hasNext()) {
      Row row = table.next();
      Object[] values = row.getValues();
      List<Object> hashValues = new ArrayList<>();
      for (int index : colIndex) {
        if (values[index] instanceof byte[]) {
          hashValues.add(new String((byte[]) values[index]));
        } else {
          hashValues.add(values[index]);
        }
      }

      GroupByKey key = new GroupByKey(hashValues);
      groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
    }
    return groups;
  }

  private static Map<GroupByKey, List<Row>> parallelBuild(Table table, int[] colIndex)
      throws PhysicalException {
    List<Row> rows = table.getRows();
    ForkJoinPool pool = null;
    try {
      pool = poolQueue.take();
      Map<GroupByKey, List<Row>> groups =
          pool.submit(
                  () ->
                      Collections.synchronizedList(rows)
                          .parallelStream()
                          .collect(
                              Collectors.groupingBy(
                                  row -> {
                                    Object[] values = row.getValues();
                                    List<Object> hashValues = new ArrayList<>();
                                    for (int index : colIndex) {
                                      if (values[index] instanceof byte[]) {
                                        hashValues.add(new String((byte[]) values[index]));
                                      } else {
                                        hashValues.add(values[index]);
                                      }
                                    }
                                    return new GroupByKey(hashValues);
                                  })))
              .get();
      return groups;
    } catch (InterruptedException | ExecutionException e) {
      throw new PhysicalException("parallel build failed");
    } finally {
      if (pool != null) {
        poolQueue.add(pool);
      }
    }
  }

  public static List<Row> cacheFilterResult(List<Row> rows, Filter filter)
      throws PhysicalException {
    List<Row> result;
    String use;
    long startTime = System.currentTimeMillis();
    if (config.isEnableParallelOperator() && rows.size() > config.getParallelFilterThreshold()) {
      use = "parallel";
      ForkJoinPool pool = null;
      try {
        pool = poolQueue.take();
        result =
            rows.parallelStream()
                .filter(
                    row -> {
                      try {
                        return FilterUtils.validate(filter, row);
                      } catch (PhysicalException e) {
                        logger.error("execute parallel filter error, cause by: ", e.getCause());
                        return false;
                      }
                    })
                .collect(Collectors.toList());
      } catch (InterruptedException e) {
        throw new PhysicalException("parallel filter failed");
      } finally {
        if (pool != null) {
          poolQueue.add(pool);
        }
      }
    } else {
      use = "sequence";
      result =
          rows.stream()
              .filter(
                  row -> {
                    try {
                      return FilterUtils.validate(filter, row);
                    } catch (PhysicalException e) {
                      logger.error("execute sequence filter error, cause by: ", e.getCause());
                      return false;
                    }
                  })
              .collect(Collectors.toList());
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        String.format(
            "select use %s filter, row size: %s, cost time: %s",
            use, rows.size(), endTime - startTime));
    return result;
  }

  public static void sortRows(List<Row> rows, boolean asc, List<String> sortByCols)
      throws PhysicalTaskExecuteFailureException {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    if (sortByCols == null || sortByCols.isEmpty()) {
      return;
    }
    Header header = rows.get(0).getHeader();

    List<Integer> indexList = new ArrayList<>();
    List<DataType> typeList = new ArrayList<>();
    boolean hasKey = false;
    for (String col : sortByCols) {
      if (col.equals(Constants.KEY)) {
        hasKey = true;
        continue;
      }
      int index = header.indexOf(col);
      if (index == -1) {
        throw new PhysicalTaskExecuteFailureException(
            String.format("SortBy key [%s] doesn't exist in table.", col));
      }
      indexList.add(index);
      typeList.add(header.getField(index).getType());
    }

    boolean finalHasKey = hasKey;
    rows.sort(
        (a, b) -> {
          if (finalHasKey) {
            int cmp =
                asc ? Long.compare(a.getKey(), b.getKey()) : Long.compare(b.getKey(), a.getKey());
            if (cmp != 0) {
              return cmp;
            }
          }
          for (int i = 0; i < indexList.size(); i++) {
            int cmp =
                asc
                    ? ValueUtils.compare(
                        a.getValue(indexList.get(i)), b.getValue(indexList.get(i)), typeList.get(i))
                    : ValueUtils.compare(
                        b.getValue(indexList.get(i)),
                        a.getValue(indexList.get(i)),
                        typeList.get(i));
            if (cmp != 0) {
              return cmp;
            }
          }
          return 0;
        });
  }

  public static List<Row> removeDuplicateRows(List<Row> rows) throws PhysicalException {
    List<Row> targetRows = new ArrayList<>();
    HashMap<Integer, List<Row>> rowsHashMap = new HashMap<>();
    List<Row> nullValueRows = new ArrayList<>();
    tableScan:
    for (Row row : rows) {
      Value value = row.getAsValue(row.getField(0).getName());
      if (value.isNull()) {
        for (Row nullValueRow : nullValueRows) {
          if (isEqualRow(row, nullValueRow, false)) {
            continue tableScan;
          }
        }
        nullValueRows.add(row);
        targetRows.add(row);
      } else {
        int hash;
        if (value.getDataType() == DataType.BINARY) {
          hash = Arrays.hashCode(value.getBinaryV());
        } else {
          hash = value.getValue().hashCode();
        }
        if (rowsHashMap.containsKey(hash)) {
          List<Row> rowsExist = rowsHashMap.get(hash);
          for (Row rowExist : rowsExist) {
            if (isEqualRow(row, rowExist, false)) {
              continue tableScan;
            }
          }
          rowsExist.add(row);
        } else {
          rowsHashMap.put(hash, new ArrayList<>(Collections.singletonList(row)));
        }
        targetRows.add(row);
      }
    }

    return targetRows;
  }
}
