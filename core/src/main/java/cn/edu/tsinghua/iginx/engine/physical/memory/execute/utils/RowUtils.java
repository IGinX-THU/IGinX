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

import static cn.edu.tsinghua.iginx.thrift.DataType.BOOLEAN;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
                columnList.get(0).getHeader().hasKey()
                        ? new Header(Field.KEY, fields)
                        : new Header(fields);
        return new Row(newHeader, columnList.get(0).getKey(), valuesCombine.toArray());
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
                    row1.getField(row1.getHeader().indexOf(prefix1 + '.' + columns1.get(index)))
                            .getType();
            DataType dataType2 =
                    row2.getField(row2.getHeader().indexOf(prefix2 + '.' + columns2.get(index)))
                            .getType();
            int cmp = ValueUtils.compare(value1, value2, dataType1, dataType2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public static Header constructNewHead(Header header, String markColumn) {
        List<Field> fields = new ArrayList<>(header.getFields());
        fields.add(new Field(markColumn, BOOLEAN));
        return header.hasKey() ? new Header(Field.KEY, fields) : new Header(fields);
    }

    public static Header constructNewHead(Header headerA, Header headerB, boolean remainKeyA) {
        List<Field> fields = new ArrayList<>();
        fields.addAll(headerA.getFields());
        fields.addAll(headerB.getFields());
        return remainKeyA && headerA.hasKey() ? new Header(Field.KEY, fields) : new Header(fields);
    }

    public static Header constructNewHead(
            Header headerA, Header headerB, String prefixA, String prefixB) {
        List<Field> fields = new ArrayList<>();
        if (headerA.hasKey()) {
            fields.add(new Field(prefixA + "." + GlobalConstant.KEY_NAME, DataType.LONG));
        }
        fields.addAll(headerA.getFields());
        if (headerB.hasKey()) {
            fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
        }
        fields.addAll(headerB.getFields());
        return new Header(fields);
    }

    public static Pair<int[], Header> constructNewHead(
            Header headerA,
            Header headerB,
            String prefixA,
            String prefixB,
            List<String> joinColumns,
            boolean cutRight) {
        List<Field> fieldsA = headerA.getFields();
        List<Field> fieldsB = headerB.getFields();
        int[] indexOfJoinColumnsInTable = new int[joinColumns.size()];

        List<Field> fields = new ArrayList<>();
        if (headerA.hasKey()) {
            fields.add(new Field(prefixA + "." + GlobalConstant.KEY_NAME, DataType.LONG));
        }
        if (cutRight) {
            fields.addAll(fieldsA);
            if (headerB.hasKey()) {
                fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
            }
            int i = 0;
            flag:
            for (Field fieldB : fieldsB) {
                for (String joinColumn : joinColumns) {
                    if (Objects.equals(fieldB.getName(), prefixB + '.' + joinColumn)) {
                        indexOfJoinColumnsInTable[i++] = headerB.indexOf(fieldB);
                        continue flag;
                    }
                }
                fields.add(fieldB);
            }
        } else {
            int i = 0;
            flag:
            for (Field fieldA : fieldsA) {
                for (String joinColumn : joinColumns) {
                    if (Objects.equals(fieldA.getName(), prefixA + '.' + joinColumn)) {
                        indexOfJoinColumnsInTable[i++] = headerA.indexOf(fieldA);
                        continue flag;
                    }
                }
                fields.add(fieldA);
            }
            if (headerB.hasKey()) {
                fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
            }
            fields.addAll(fieldsB);
        }
        return new Pair<>(indexOfJoinColumnsInTable, new Header(fields));
    }

    public static Row constructUnmatchedRow(
            Header header, Row halfRow, int anotherRowSize, boolean putLeft) {

        int size = halfRow.getValues().length + anotherRowSize;
        if (halfRow.getHeader().hasKey()) {
            size++;
        }
        Object[] valuesJoin = new Object[size];

        if (putLeft) {
            if (halfRow.getHeader().hasKey()) {
                valuesJoin[0] = halfRow.getKey();
                System.arraycopy(halfRow.getValues(), 0, valuesJoin, 1, halfRow.getValues().length);
            } else {
                System.arraycopy(halfRow.getValues(), 0, valuesJoin, 0, halfRow.getValues().length);
            }
        } else {
            if (halfRow.getHeader().hasKey()) {
                valuesJoin[anotherRowSize] = halfRow.getKey();
                System.arraycopy(
                        halfRow.getValues(),
                        0,
                        valuesJoin,
                        anotherRowSize + 1,
                        halfRow.getValues().length);
            } else {
                System.arraycopy(
                        halfRow.getValues(),
                        0,
                        valuesJoin,
                        anotherRowSize,
                        halfRow.getValues().length);
            }
        }
        return new Row(header, valuesJoin);
    }

    public static Row constructNewRow(Header header, Row rowA, Row rowB, boolean remainKeyA) {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();

        int size = valuesA.length + valuesB.length;
        int rowAStartIndex = 0, rowBStartIndex = valuesA.length;

        Object[] valuesJoin = new Object[size];

        System.arraycopy(valuesA, 0, valuesJoin, rowAStartIndex, valuesA.length);
        System.arraycopy(valuesB, 0, valuesJoin, rowBStartIndex, valuesB.length);
        return remainKeyA && rowA.getHeader().hasKey()
                ? new Row(header, rowA.getKey(), valuesJoin)
                : new Row(header, valuesJoin);
    }

    public static Row constructNewRow(Header header, Row rowA, Row rowB) {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();

        int size = valuesA.length + valuesB.length;
        int rowAStartIndex = 0, rowBStartIndex = valuesA.length;
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
        System.arraycopy(valuesA, 0, valuesJoin, rowAStartIndex, valuesA.length);
        System.arraycopy(valuesB, 0, valuesJoin, rowBStartIndex, valuesB.length);
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
            List<String> joinColumns,
            Header headerA,
            Header headerB,
            String prefixA,
            String prefixB)
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
            throws PhysicalTaskExecuteFailureException {
        List<FunctionCall> functionCallList = groupBy.getFunctionCallList();
        for (FunctionCall functionCall : functionCallList) {
            SetMappingFunction function = (SetMappingFunction) functionCall.getFunction();
            FunctionParams params = functionCall.getParams();

            boolean hasAddedFields = false;
            for (Map.Entry<GroupByKey, List<Row>> entry : groups.entrySet()) {
                List<Row> group = entry.getValue();
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
                            "encounter error when execute set mapping function "
                                    + function.getIdentifier()
                                    + ".",
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
                            groups.entrySet()
                                    .parallelStream()
                                    .forEach(
                                            entry -> {
                                                List<Row> group = entry.getValue();
                                                try {
                                                    Row row =
                                                            function.transform(
                                                                    new Table(header, group),
                                                                    params);
                                                    if (row != null) {
                                                        entry.getKey()
                                                                .getFuncRet()
                                                                .addAll(
                                                                        Arrays.asList(
                                                                                row.getValues()));
                                                        if (hasAddedFields.compareAndSet(
                                                                false, true)) {
                                                            fields.addAll(
                                                                    row.getHeader().getFields());
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    logger.error(
                                                            "encounter error when execute set mapping function ");
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
                                                                        Object[] values =
                                                                                row.getValues();
                                                                        List<Object> hashValues =
                                                                                new ArrayList<>();
                                                                        for (int index : colIndex) {
                                                                            if (values[index]
                                                                                    instanceof
                                                                                    byte[]) {
                                                                                hashValues.add(
                                                                                        new String(
                                                                                                (byte
                                                                                                                [])
                                                                                                        values[
                                                                                                                index]));
                                                                            } else {
                                                                                hashValues.add(
                                                                                        values[
                                                                                                index]);
                                                                            }
                                                                        }
                                                                        return new GroupByKey(
                                                                                hashValues);
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
                                asc
                                        ? Long.compare(a.getKey(), b.getKey())
                                        : Long.compare(b.getKey(), a.getKey());
                        if (cmp != 0) {
                            return cmp;
                        }
                    }
                    for (int i = 0; i < indexList.size(); i++) {
                        int cmp =
                                asc
                                        ? ValueUtils.compare(
                                                a.getValue(indexList.get(i)),
                                                b.getValue(indexList.get(i)),
                                                typeList.get(i))
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
}
