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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
import cn.edu.tsinghua.iginx.engine.shared.function.MappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.First;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Last;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Min;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(RowUtils.class);

  private static final BlockingQueue<ForkJoinPool> poolQueue = new LinkedBlockingQueue<>();

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

  public static HashMap<Integer, List<Row>> establishHashMap(
      List<Row> rows, String joinPath, boolean needTypeCast) {
    HashMap<Integer, List<Row>> hashMap = new HashMap<>();
    for (Row row : rows) {
      Value value = row.getAsValue(joinPath);
      if (value == null || value.getValue() == null) {
        continue;
      }
      int hash = getHash(value, needTypeCast);

      List<Row> l = hashMap.computeIfAbsent(hash, k -> new ArrayList<>());
      l.add(row);
    }
    return hashMap;
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
    if (table.getRowSize() > config.getParallelGroupByRowsThreshold()) {
      groups = parallelBuild(table, colIndex);
    } else {
      groups = seqBuild(table, colIndex);
    }

    return applyFunc(groupBy, fields, header, groups);
  }

  public static List<Row> applyFunc(
      GroupBy groupBy, List<Field> fields, Header header, Map<GroupByKey, List<Row>> groups)
      throws PhysicalException {

    if (groups.size() > config.getParallelApplyFuncGroupsThreshold()) {
      parallelApplyFunc(groupBy, fields, header, groups);
    } else {
      seqApplyFunc(groupBy, fields, header, groups);
    }

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
                          LOGGER.error("encounter error when execute set mapping function ");
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
    if (rows.size() > config.getParallelFilterThreshold()) {
      ForkJoinPool pool = null;
      try {
        pool = poolQueue.take();
        return rows.parallelStream()
            .filter(
                row -> {
                  try {
                    return FilterUtils.validate(filter, row);
                  } catch (PhysicalException e) {
                    LOGGER.error("execute parallel filter error, cause by: ", e.getCause());
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
      return rows.stream()
          .filter(
              row -> {
                try {
                  return FilterUtils.validate(filter, row);
                } catch (PhysicalException e) {
                  LOGGER.error("execute sequence filter error, cause by: ", e.getCause());
                  return false;
                }
              })
          .collect(Collectors.toList());
    }
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

  /**
   * 将多个table进行PathUnion,在Last、First中使用到
   *
   * @param tableList table列表
   * @return 合并后的table
   * @throws PhysicalException Table列表为空或者Table有的有key有的没有key时，抛出异常
   */
  public static Table pathUnionMultipleTables(List<Table> tableList) throws PhysicalException {
    if (tableList == null || tableList.isEmpty()) {
      throw new IllegalArgumentException("Table list cannot be null or empty");
    }

    // 检查每个表的头部是否包含键
    boolean hasKey = tableList.get(0).getHeader().hasKey();
    for (Table table : tableList) {
      if (table.getHeader().hasKey() != hasKey) {
        throw new InvalidOperatorParameterException(
            "All tables in the union must have the same key configuration");
      }
    }

    Set<Field> targetFieldSet = new LinkedHashSet<>();
    for (Table table : tableList) {
      targetFieldSet.addAll(table.getHeader().getFields());
    }
    List<Field> targetFields = new ArrayList<>(targetFieldSet);

    Header targetHeader = hasKey ? new Header(Field.KEY, targetFields) : new Header(targetFields);
    List<Row> rows = new ArrayList<>();

    if (!hasKey) {
      for (Table table : tableList) {
        for (Row row : table.getRows()) {
          rows.add(RowUtils.transform(row, targetHeader));
        }
      }
    } else {
      // PriorityQueue中的Pair，k为行，v为表格在tableList中的索引（即记录该行所属的table）
      PriorityQueue<Pair<Row, Integer>> queue =
          new PriorityQueue<>(Comparator.comparingLong(p -> p.k.getKey()));

      // 初始化优先队列
      for (int i = 0; i < tableList.size(); i++) {
        if (tableList.get(i).getRowSize() > 0) {
          queue.add(new Pair<>(tableList.get(i).next(), i));
        }
      }

      // 合并行
      while (!queue.isEmpty()) {
        Pair<Row, Integer> entry = queue.poll();
        Row row = entry.k;
        int tableIndex = entry.v;
        rows.add(RowUtils.transform(row, targetHeader));

        // 如果该表格还有更多行，将下一行加入队列
        if (tableList.get(tableIndex).hasNext()) {
          queue.add(new Pair<>(tableList.get(tableIndex).next(), tableIndex));
        }
      }
    }

    // 重置所有table的迭代器，以便下次使用
    for (Table table : tableList) {
      table.reset();
    }

    return new Table(targetHeader, rows);
  }

  /**
   * 将多个table进行Join Ordinal，SetMappingTransform中用到
   *
   * @param tableList table列表
   * @return 合并后的table
   * @throws PhysicalException Table列表为空或者Table有Key时，抛出异常
   */
  public static Table joinMultipleTablesByOrdinal(List<Table> tableList) throws PhysicalException {
    if (tableList == null || tableList.isEmpty()) {
      throw new IllegalArgumentException("Table list cannot be null or empty");
    }

    if (tableList.size() == 1) {
      return tableList.get(0);
    }

    // 检查每个表的头部是否包含键
    for (Table table : tableList) {
      if (table.getHeader().hasKey()) {
        throw new InvalidOperatorParameterException("All tables in the join cannot have key");
      }
    }

    Map<Table, Integer> tableIndexMap = new HashMap<>(); // 记录每个表格在大表格中列的起始位置
    List<Field> newFields = new ArrayList<>();
    for (Table table : tableList) {
      tableIndexMap.put(table, newFields.size());
      newFields.addAll(table.getHeader().getFields());
    }

    Header newHeader = new Header(newFields);
    List<Row> newRows = new ArrayList<>();

    int maxRowCount = 0;
    for (Table table : tableList) {
      maxRowCount = Math.max(maxRowCount, table.getRowSize());
    }

    for (int i = 0; i < maxRowCount; i++) {
      Object[] values = new Object[newHeader.getFieldSize()];
      for (Table table : tableList) {
        if (i < table.getRowSize()) {
          System.arraycopy(
              table.getRow(i).getValues(),
              0,
              values,
              tableIndexMap.get(table),
              table.getHeader().getFieldSize());
        }
      }
      newRows.add(new Row(newHeader, values));
    }

    return new Table(newHeader, newRows);
  }

  /**
   * 将多个table进行Join By Key，DownSample中用到
   *
   * @param tableList table列表
   * @return 合并后的table
   * @throws PhysicalException Table列表为空或者Table不含Key时，抛出异常
   */
  public static Table joinMultipleTablesByKey(List<Table> tableList) throws PhysicalException {
    if (tableList == null || tableList.isEmpty()) {
      throw new IllegalArgumentException("Table list cannot be null or empty");
    }

    // 检查时间戳
    for (Table table : tableList) {
      if (!table.getHeader().hasKey()) {
        throw new InvalidOperatorParameterException(
            "row streams for join operator by time should have timestamp.");
      }
    }

    // 构造表头
    List<Field> newFields = new ArrayList<>();
    Map<Table, Integer> tableIndexMap = new HashMap<>(); // 记录每个表格在大表格中列的起始位置
    for (Table table : tableList) {
      tableIndexMap.put(table, newFields.size());
      newFields.addAll(table.getHeader().getFields());
    }
    Header newHeader = new Header(Field.KEY, newFields);
    List<Row> newRows = new ArrayList<>();

    // PriorityQueue中的Pair，k为行，v为所属表格
    PriorityQueue<Pair<Row, Table>> queue =
        new PriorityQueue<>(Comparator.comparingLong(p -> p.k.getKey()));

    // 初始化优先队列，把每个表格的第一行加入队列
    for (int i = 0; i < tableList.size(); i++) {
      if (tableList.get(i).getRowSize() > 0) {
        queue.add(new Pair<>(tableList.get(i).next(), tableList.get(i)));
      }
    }

    while (!queue.isEmpty()) {
      // 获取当前堆顶的key, 即当前最小的时间戳，不弹出
      long curKey = queue.peek().k.getKey();
      Object[] values = new Object[newHeader.getFieldSize()];
      // 从堆顶开始，弹出所有时间戳相同的行，copy到新行中
      while (!queue.isEmpty() && queue.peek().k.getKey() == curKey) {
        Pair<Row, Table> entry = queue.poll();
        Row row = entry.k;
        Table table = entry.v;
        System.arraycopy(
            row.getValues(), 0, values, tableIndexMap.get(table), table.getHeader().getFieldSize());

        // 如果该表格还有更多行，将下一行加入队列
        if (table.hasNext()) {
          queue.add(new Pair<>(table.next(), table));
        }
      }

      // 将新行加入结果表
      newRows.add(new Row(newHeader, curKey, values));
    }

    // 重置所有table的迭代器，以便下次使用
    for (Table table : tableList) {
      table.reset();
    }

    return new Table(newHeader, newRows);
  }

  /**
   * 计算多个MappingTransform的结果
   *
   * @param table 输入表
   * @param functionCallList MappingTransform的FunctionCall列表
   * @return 计算结果输出表格
   * @throws PhysicalException 当FunctionCall列表中有非MappingTransform时，抛出异常；当执行MappingTransform时出错时，抛出异常
   */
  public static Table calMappingTransform(Table table, List<FunctionCall> functionCallList)
      throws PhysicalException {
    List<Table> tableList = new ArrayList<>();
    for (FunctionCall functionCall : functionCallList) {
      FunctionParams params = functionCall.getParams();
      if (!(functionCall.getFunction() instanceof MappingFunction)) {
        throw new PhysicalTaskExecuteFailureException(
            "function: "
                + functionCall.getFunction().getIdentifier()
                + " is not a mapping function");
      }
      MappingFunction function = (MappingFunction) functionCall.getFunction();

      try {
        Table functable = (Table) function.transform(table, params);
        if (functable != null) {
          tableList.add(functable);
        }
      } catch (Exception e) {
        throw new PhysicalTaskExecuteFailureException(
            "encounter error when execute mapping function " + function.getIdentifier() + ".", e);
      }
    }

    if (tableList.isEmpty()) {
      return Table.EMPTY_TABLE;
    }

    // 如果是First/Last，用PathUnion合并表格；如果是GroupBy,用Join Ordinal合并表格
    boolean isFirstLast = false;
    for (FunctionCall functionCall : functionCallList) {
      if (functionCall.getFunction().getIdentifier().equals(First.FIRST)
          || functionCall.getFunction().getIdentifier().equals(Last.LAST)) {
        isFirstLast = true;
        break;
      }
    }

    if (isFirstLast) {
      return RowUtils.pathUnionMultipleTables(tableList);
    } else {
      return RowUtils.joinMultipleTablesByOrdinal(tableList);
    }
  }
}
