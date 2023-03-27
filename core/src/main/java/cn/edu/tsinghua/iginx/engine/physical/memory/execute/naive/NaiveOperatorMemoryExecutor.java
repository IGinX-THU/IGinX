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

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Downsample;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Join;
import cn.edu.tsinghua.iginx.engine.shared.operator.Limit;
import cn.edu.tsinghua.iginx.engine.shared.operator.MappingTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
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
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
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

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.getJoinPathFromFilter;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.combineMultipleColumns;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.constructNewHead;

public class NaiveOperatorMemoryExecutor implements OperatorMemoryExecutor {

    private NaiveOperatorMemoryExecutor() {
    }

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
                return executeMappingTransform((MappingTransform) operator,
                    transformToTable(stream));
            case Rename:
                return executeRename((Rename) operator, transformToTable(stream));
            case Reorder:
                return executeReorder((Reorder) operator, transformToTable(stream));
            case AddSchemaPrefix:
                return executeAddSchemaPrefix((AddSchemaPrefix) operator, transformToTable(stream));
            case GroupBy:
                return executeGroupBy((GroupBy) operator, stream);
            default:
                throw new UnexpectedOperatorException(
                    "unknown unary operator: " + operator.getType());
        }
    }

    @Override
    public RowStream executeBinaryOperator(BinaryOperator operator, RowStream streamA,
        RowStream streamB) throws PhysicalException {
        switch (operator.getType()) {
            case Join:
                return executeJoin((Join) operator, transformToTable(streamA),
                    transformToTable(streamB));
            case CrossJoin:
                return executeCrossJoin((CrossJoin) operator, transformToTable(streamA),
                    transformToTable(streamB));
            case InnerJoin:
                return executeInnerJoin((InnerJoin) operator, transformToTable(streamA),
                    transformToTable(streamB));
            case OuterJoin:
                return executeOuterJoin((OuterJoin) operator, transformToTable(streamA),
                    transformToTable(streamB));
            case SingleJoin:
                return executeSingleJoin((SingleJoin) operator, transformToTable(streamA),
                    transformToTable(streamB));
            case MarkJoin:
                return executeMarkJoin((MarkJoin) operator, transformToTable(streamA),
                    transformToTable(streamB));
            case Union:
                return executeUnion((Union) operator, transformToTable(streamA),
                    transformToTable(streamB));
            default:
                throw new UnexpectedOperatorException(
                    "unknown unary operator: " + operator.getType());
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
        List<Row> targetRows = new ArrayList<>();
        while (table.hasNext()) {
            Row row = table.next();
            if (FilterUtils.validate(filter, row)) {
                targetRows.add(row);
            }
        }
        return new Table(table.getHeader(), targetRows);
    }

    private RowStream executeSort(Sort sort, Table table) throws PhysicalException {
        RowUtils.sortRows(
            table.getRows(),
            sort.getSortType() == SortType.ASC,
            sort.getSortByCols());
        return table;
    }

    private RowStream executeLimit(Limit limit, Table table) throws PhysicalException {
        int rowSize = table.getRowSize();
        Header header = table.getHeader();
        List<Row> rows = new ArrayList<>();
        if (rowSize > limit.getOffset()) { // 没有把所有的行都跳过
            for (int i = limit.getOffset(); i < rowSize && i - limit.getOffset() < limit.getLimit();
                i++) {
                rows.add(table.getRow(i));
            }
        }
        return new Table(header, rows);
    }

    private RowStream executeDownsample(Downsample downsample, Table table)
        throws PhysicalException {
        Header header = table.getHeader();
        if (!header.hasKey()) {
            throw new InvalidOperatorParameterException(
                "downsample operator is not support for row stream without timestamps.");
        }
        List<Row> rows = table.getRows();
        long bias = downsample.getTimeRange().getActualBeginTime();
        long endTime = downsample.getTimeRange().getActualEndTime();
        long precision = downsample.getPrecision();
        long slideDistance = downsample.getSlideDistance();
        // startTime + (n - 1) * slideDistance + precision - 1 >= endTime
        int n = (int) (Math.ceil((double) (endTime - bias - precision + 1) / slideDistance) + 1);
        TreeMap<Long, List<Row>> groups = new TreeMap<>();
        SetMappingFunction function = (SetMappingFunction) downsample.getFunctionCall()
            .getFunction();
        Map<String, Value> params = downsample.getFunctionCall().getParams();
        if (precision == slideDistance) {
            for (Row row : rows) {
                long timestamp = row.getKey() - (row.getKey() - bias) % precision;
                groups.compute(timestamp, (k, v) -> v == null ? new ArrayList<>() : v).add(row);
            }
        } else {
            long[] timestamps = new long[n];
            for (int i = 0; i < n; i++) {
                timestamps[i] = bias + i * slideDistance;
            }
            for (Row row : rows) {
                long rowTimestamp = row.getKey();
                for (int i = 0; i < n; i++) {
                    if (rowTimestamp - timestamps[i] >= 0
                        && rowTimestamp - timestamps[i] < precision) {
                        groups.compute(timestamps[i], (k, v) -> v == null ? new ArrayList<>() : v)
                            .add(row);
                    }
                }
            }
        }
        List<Pair<Long, Row>> transformedRawRows = new ArrayList<>();
        try {
            for (Map.Entry<Long, List<Row>> entry : groups.entrySet()) {
                long time = entry.getKey();
                List<Row> group = entry.getValue();
                Row row = function.transform(new Table(header, group), params);
                if (row != null) {
                    transformedRawRows.add(new Pair<>(time, row));
                }
            }
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException(
                "encounter error when execute set mapping function " + function.getIdentifier()
                    + ".", e);
        }
        if (transformedRawRows.size() == 0) {
            return Table.EMPTY_TABLE;
        }
        Header newHeader = new Header(Field.KEY,
            transformedRawRows.get(0).v.getHeader().getFields());
        List<Row> transformedRows = new ArrayList<>();
        for (Pair<Long, Row> pair : transformedRawRows) {
            transformedRows.add(new Row(newHeader, pair.k, pair.v.getValues()));
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeRowTransform(RowTransform rowTransform, Table table)
        throws PhysicalException {
        List<Pair<RowMappingFunction, Map<String, Value>>> list = new ArrayList<>();
        rowTransform.getFunctionCallList().forEach(functionCall -> {
            list.add(new Pair<>((RowMappingFunction) functionCall.getFunction(), functionCall.getParams()));
        });
        
        List<Row> rows = new ArrayList<>();
        while (table.hasNext()) {
            Row current = table.next();
            List<Row> columnList = new ArrayList<>();
            list.forEach(pair -> {
                RowMappingFunction function = pair.k;
                Map<String, Value> params = pair.v;
                try {
                    // 分别计算每个表达式得到相应的结果
                    Row column = function.transform(current, params);
                    if (column != null) {
                        columnList.add(column);
                    }
                } catch (Exception e) {
                    try {
                        throw new PhysicalTaskExecuteFailureException(
                                "encounter error when execute row mapping function " + function.getIdentifier()
                                        + ".", e);
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
        SetMappingFunction function = (SetMappingFunction) setTransform.getFunctionCall()
            .getFunction();
        Map<String, Value> params = setTransform.getFunctionCall().getParams();
        try {
            Row row = function.transform(table, params);
            if (row == null) {
                return Table.EMPTY_TABLE;
            }
            Header header = row.getHeader();
            return new Table(header, Collections.singletonList(row));
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException(
                "encounter error when execute set mapping function " + function.getIdentifier()
                    + ".", e);
        }

    }

    private RowStream executeMappingTransform(MappingTransform mappingTransform, Table table)
        throws PhysicalException {
        MappingFunction function = (MappingFunction) mappingTransform.getFunctionCall()
            .getFunction();
        Map<String, Value> params = mappingTransform.getFunctionCall().getParams();
        try {
            return function.transform(table, params);
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException(
                "encounter error when execute mapping function " + function.getIdentifier() + ".",
                e);
        }
    }

    private RowStream executeRename(Rename rename, Table table) throws PhysicalException {
        Header header = table.getHeader();
        Map<String, String> aliasMap = rename.getAliasMap();

        List<Field> fields = new ArrayList<>();
        header.getFields().forEach(field -> {
            String alias = "";
            for (String oldName : aliasMap.keySet()) {
                if (Objects.equals(oldName, "*") && aliasMap.get(oldName).endsWith(".*")) {
                    String newPrefix = aliasMap.get(oldName).replace("*", "");
                    alias = newPrefix + field.getFullName();
                } else if (oldName.endsWith(".*") && aliasMap.get(oldName).endsWith(".*")) {
                    String oldPrefix = oldName.replace(".*", "");
                    String newPrefix = aliasMap.get(oldName).replace(".*", "");
                    if (field.getFullName().startsWith(oldPrefix)) {
                        alias = field.getFullName().replaceFirst(oldPrefix, newPrefix);
                    }
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
        table.getRows().forEach(row -> {
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
        header.getFields().forEach(field -> {
            if (schemaPrefix != null) {
                fields.add(new Field(schemaPrefix + "." + field.getName(), field.getType(),
                    field.getTags()));
            } else {
                fields.add(new Field(field.getName(), field.getType(), field.getTags()));
            }
        });

        Header newHeader = new Header(header.getKey(), fields);

        List<Row> rows = new ArrayList<>();
        table.getRows().forEach(row -> {
            if (newHeader.hasKey()) {
                rows.add(new Row(newHeader, row.getKey(), row.getValues()));
            } else {
                rows.add(new Row(newHeader, row.getValues()));
            }
        });

        return new Table(newHeader, rows);
    }

    private RowStream executeGroupBy(GroupBy groupBy, RowStream stream) throws PhysicalException {
        List<Row> rows = RowUtils.cacheGroupByResult(groupBy, stream);
        if (rows.isEmpty()) {
            return Table.EMPTY_TABLE;
        }
        Header header = rows.get(0).getHeader();
        return new Table(header, rows);
    }

    private RowStream executeReorder(Reorder reorder, Table table) throws PhysicalException {
        List<String> patterns = reorder.getPatterns();
        Header header = table.getHeader();
        List<Field> targetFields = new ArrayList<>();
        Map<Integer, Integer> reorderMap = new HashMap<>();

        for (String pattern : patterns) {
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
                matchedFields.forEach(pair -> {
                    reorderMap.put(targetFields.size(), pair.getV());
                    targetFields.add(pair.getK());
                });
            }
        }

        Header newHeader = new Header(header.getKey(), targetFields);
        List<Row> rows = new ArrayList<>();
        table.getRows().forEach(row -> {
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

    private RowStream executeJoin(Join join, Table tableA, Table tableB) throws PhysicalException {
        boolean hasIntersect = false;
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
        // 检查 field，暂时不需要
        for (Field field : headerA.getFields()) {
            if (headerB.indexOf(field) != -1) { // 二者的 field 存在交集
                hasIntersect = true;
                break;
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
                    System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(),
                        headerB.getFieldSize());
                    index1++;
                    index2++;
                } else if (rowA.getKey() < rowB.getKey()) {
                    timestamp = rowA.getKey();
                    System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
                    index1++;
                } else {
                    timestamp = rowB.getKey();
                    System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(),
                        headerB.getFieldSize());
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
                System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(),
                    headerB.getFieldSize());
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
                System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(),
                    headerB.getFieldSize());
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
                System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(),
                    headerB.getFieldSize());
                newRows.add(new Row(newHeader, values));
            }
            return new Table(newHeader, newRows);
        } else {
            throw new InvalidOperatorParameterException(
                "join operator is not support for field " + join.getJoinBy() + " except for "
                    + Constants.KEY
                    + " and " + Constants.ORDINAL);
        }
    }

    private RowStream executeCrossJoin(CrossJoin crossJoin, Table tableA, Table tableB)
        throws PhysicalException {
        Header newHeader = RowUtils
            .constructNewHead(tableA.getHeader(), tableB.getHeader(), crossJoin.getPrefixA(),
                crossJoin.getPrefixB());

        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();

        List<Row> transformedRows = new ArrayList<>();
        for (Row rowA : rowsA) {
            for (Row rowB : rowsB) {
                Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, rowB);
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
                throw new PhysicalException(
                    "Unknown join algorithm type: " + innerJoin.getJoinAlgType());
        }
    }

    private RowStream executeNestedLoopInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB)
        throws PhysicalException {
        Filter filter = innerJoin.getFilter();
        List<String> joinColumns = new ArrayList<>(innerJoin.getJoinColumns());

        if (innerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, tableA.getHeader(), tableB.getHeader(),
                innerJoin.getPrefixA(), innerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns
            .isEmpty())) {
            throw new InvalidOperatorParameterException(
                "using(or natural) and on operator cannot be used at the same time");
        }

        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();

        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) { // Join condition: on
            newHeader = RowUtils
                .constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());
            for (Row rowA : rowsA) {
                for (Row rowB : rowsB) {
                    Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, rowB);
                    if (FilterUtils.validate(filter, joinedRow)) {
                        transformedRows.add(joinedRow);
                    }
                }
            }
        } else { // Join condition: natural or using
            Pair<int[], Header> pair = RowUtils
                .constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB(),
                    joinColumns, true);
            int[] indexOfJoinColumnInTableB = pair.getK();
            newHeader = pair.getV();
            for (Row rowA : rowsA) {
                flag:
                for (Row rowB : rowsB) {
                    for (String joinColumn : joinColumns) {
                        if (ValueUtils
                            .compare(rowA.getAsValue(innerJoin.getPrefixA() + '.' + joinColumn),
                                rowB.getAsValue(innerJoin.getPrefixB() + '.' + joinColumn)) != 0) {
                            continue flag;
                        }
                    }
                    Row joinedRow = RowUtils
                        .constructNewRow(newHeader, rowA, rowB, indexOfJoinColumnInTableB, true);
                    transformedRows.add(joinedRow);
                }
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeHashInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB)
        throws PhysicalException {
        Filter filter = innerJoin.getFilter();

        List<String> joinColumns = new ArrayList<>(innerJoin.getJoinColumns());
        if (innerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, tableA.getHeader(), tableB.getHeader(),
                innerJoin.getPrefixA(), innerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns
            .isEmpty())) {
            throw new InvalidOperatorParameterException(
                "using(or natural) and on operator cannot be used at the same time");
        }

        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();

        String joinColumnA, joinColumnB;
        if (filter != null) {
            if (!filter.getType().equals(FilterType.Path)) {
                throw new InvalidOperatorParameterException(
                    "hash join only support one path filter yet.");
            }
            Pair<String, String> p = FilterUtils.getJoinColumnFromPathFilter((PathFilter) filter);
            if (p == null) {
                throw new InvalidOperatorParameterException(
                    "hash join only support equal path filter yet.");
            }
            if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                joinColumnA = p.k.replaceFirst(innerJoin.getPrefixA() + '.', "");
                joinColumnB = p.v.replaceFirst(innerJoin.getPrefixB() + ".", "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                joinColumnA = p.v.replaceFirst(innerJoin.getPrefixA() + '.', "");
                joinColumnB = p.k.replaceFirst(innerJoin.getPrefixB() + ".", "");
            } else {
                throw new InvalidOperatorParameterException("invalid hash join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException(
                    "hash join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(innerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1
                && headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                joinColumnA = joinColumnB = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid hash join column input.");
            }
        }

        boolean needTypeCast = false;
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
        if (!rowsA.isEmpty() && !rowsB.isEmpty()) {
            Value valueA = rowsA.get(0).getAsValue(innerJoin.getPrefixA() + '.' + joinColumnA);
            Value valueB = rowsB.get(0).getAsValue(innerJoin.getPrefixB() + '.' + joinColumnB);
            if (valueA.getDataType() != valueB.getDataType()) {
                if (ValueUtils.isNumericType(valueA) && ValueUtils.isNumericType(valueB)) {
                    needTypeCast = true;
                }
            }
        }

        HashMap<Integer, List<Row>> rowsBHashMap = new HashMap<>();
        for (Row rowB : rowsB) {
            Value value = rowB.getAsValue(innerJoin.getPrefixB() + '.' + joinColumnB);
            if (value == null) {
                continue;
            }
            if (needTypeCast) {
                value = ValueUtils.transformToDouble(value);
            }
            int hash;
            if (value.getDataType() == DataType.BINARY) {
                hash = Arrays.hashCode(value.getBinaryV());
            } else {
                hash = value.getValue().hashCode();
            }
            List<Row> l =
                rowsBHashMap.containsKey(hash) ? rowsBHashMap.get(hash) : new ArrayList<>();
            l.add(rowB);
            rowsBHashMap.put(hash, l);
        }
        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) { // Join condition: on
            newHeader = RowUtils
                .constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());
            for (Row rowA : rowsA) {
                Value value = rowA.getAsValue(innerJoin.getPrefixA() + '.' + joinColumnA);
                if (value == null) {
                    continue;
                }
                if (needTypeCast) {
                    value = ValueUtils.transformToDouble(value);
                }
                int hash;
                if (value.getDataType() == DataType.BINARY) {
                    hash = Arrays.hashCode(value.getBinaryV());
                } else {
                    hash = value.getValue().hashCode();
                }

                if (rowsBHashMap.containsKey(hash)) {
                    List<Row> hashRowsB = rowsBHashMap.get(hash);
                    for (Row rowB : hashRowsB) {
                        Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, rowB);
                        if (FilterUtils.validate(filter, joinedRow)) {
                            transformedRows.add(joinedRow);
                        }
                    }
                }
            }
        } else { // Join condition: natural or using
            newHeader = RowUtils
                .constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB(),
                    Collections.singletonList(joinColumnB), true).getV();
            int index = headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumnB);
            for (Row rowA : rowsA) {
                Value value = rowA.getAsValue(innerJoin.getPrefixA() + '.' + joinColumnA);
                if (value == null) {
                    continue;
                }
                if (needTypeCast) {
                    value = ValueUtils.transformToDouble(value);
                }
                int hash;
                if (value.getDataType() == DataType.BINARY) {
                    hash = Arrays.hashCode(value.getBinaryV());
                } else {
                    hash = value.getValue().hashCode();
                }

                if (rowsBHashMap.containsKey(hash)) {
                    List<Row> hashRowsB = rowsBHashMap.get(hash);
                    for (Row rowB : hashRowsB) {
                        Row joinedRow = RowUtils
                            .constructNewRow(newHeader, rowA, rowB, new int[]{index}, true);
                        transformedRows.add(joinedRow);
                    }
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
            RowUtils.fillNaturalJoinColumns(joinColumns, tableA.getHeader(), tableB.getHeader(),
                innerJoin.getPrefixA(), innerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns
            .isEmpty())) {
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
            throw new InvalidOperatorParameterException(
                "input rows in merge join haven't be sorted.");
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
            newHeader = RowUtils
                .constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());
            int indexA = 0;
            int indexB = 0;
            int startIndexOfContinuousEqualValuesB = 0;
            while (indexA < rowsA.size() && indexB < rowsB.size()) {
                int flagAEqualB = RowUtils
                    .compareRowsSortedByColumns(rowsA.get(indexA), rowsB.get(indexB),
                        innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumnsA, joinColumnsB);
                if (flagAEqualB == 0) {
                    Row joinedRow = RowUtils
                        .constructNewRow(newHeader, rowsA.get(indexA), rowsB.get(indexB));
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
                            int flagAEqualNextA = RowUtils
                                .compareRowsSortedByColumns(rowsA.get(indexA),
                                    rowsA.get(indexA + 1), innerJoin.getPrefixA(),
                                    innerJoin.getPrefixA(), joinColumnsA, joinColumnsA);
                            int flagBEqualNextB = RowUtils
                                .compareRowsSortedByColumns(rowsB.get(indexB),
                                    rowsB.get(indexB + 1), innerJoin.getPrefixB(),
                                    innerJoin.getPrefixB(), joinColumnsB, joinColumnsB);
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
            Pair<int[], Header> pair = RowUtils
                .constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB(),
                    joinColumns, true);
            int[] indexOfJoinColumnInTableB = pair.getK();
            newHeader = pair.getV();
            int indexA = 0;
            int indexB = 0;
            int startIndexOfContinuousEqualValuesB = 0;
            while (indexA < rowsA.size() && indexB < rowsB.size()) {
                int flagAEqualB = RowUtils
                    .compareRowsSortedByColumns(rowsA.get(indexA), rowsB.get(indexB),
                        innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumnsA, joinColumnsB);
                if (flagAEqualB == 0) {
                    Row joinedRow = RowUtils
                        .constructNewRow(newHeader, rowsA.get(indexA), rowsB.get(indexB),
                            indexOfJoinColumnInTableB, true);
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
                            int flagAEqualNextA = RowUtils
                                .compareRowsSortedByColumns(rowsA.get(indexA),
                                    rowsA.get(indexA + 1), innerJoin.getPrefixA(),
                                    innerJoin.getPrefixA(), joinColumnsA, joinColumnsA);
                            int flagBEqualNextB = RowUtils
                                .compareRowsSortedByColumns(rowsB.get(indexB),
                                    rowsB.get(indexB + 1), innerJoin.getPrefixB(),
                                    innerJoin.getPrefixB(), joinColumnsB, joinColumnsB);
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
                throw new PhysicalException(
                    "Unknown join algorithm type: " + outerJoin.getJoinAlgType());
        }
    }

    private RowStream executeNestedLoopOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB)
        throws PhysicalException {
        OuterJoinType outerType = outerJoin.getOuterJoinType();
        Filter filter = outerJoin.getFilter();

        List<String> joinColumns = new ArrayList<>(outerJoin.getJoinColumns());
        if (outerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, tableA.getHeader(), tableB.getHeader(),
                outerJoin.getPrefixA(), outerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns
            .isEmpty())) {
            throw new InvalidOperatorParameterException(
                "using(or natural) and on operator cannot be used at the same time");
        }

        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();

        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();

        Bitmap bitmapA = new Bitmap(rowsA.size());
        Bitmap bitmapB = new Bitmap(rowsB.size());

        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) { // Join condition: on
            newHeader = RowUtils
                .constructNewHead(headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());
            for (int indexA = 0; indexA < rowsA.size(); indexA++) {
                Row rowA = rowsA.get(indexA);
                for (int indexB = 0; indexB < rowsB.size(); indexB++) {
                    Row rowB = rowsB.get(indexB);
                    Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, rowB);
                    if (FilterUtils.validate(filter, joinedRow)) {
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
        } else { // Join condition: natural or using
            Pair<int[], Header> pair;
            if (outerType == OuterJoinType.RIGHT) {
                pair = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), joinColumns, false);
            } else {
                pair = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), joinColumns, true);
            }
            int[] indexOfJoinColumnInTable = pair.getK();
            newHeader = pair.getV();

            for (int indexA = 0; indexA < rowsA.size(); indexA++) {
                Row rowA = rowsA.get(indexA);
                flag:
                for (int indexB = 0; indexB < rowsB.size(); indexB++) {
                    Row rowB = rowsB.get(indexB);
                    for (String joinColumn : joinColumns) {
                        if (ValueUtils
                            .compare(rowA.getAsValue(outerJoin.getPrefixA() + '.' + joinColumn),
                                rowB.getAsValue(outerJoin.getPrefixB() + '.' + joinColumn)) != 0) {
                            continue flag;
                        }
                    }

                    Row joinedRow;
                    if (outerType == OuterJoinType.RIGHT) {
                        joinedRow = RowUtils
                            .constructNewRow(newHeader, rowA, rowB, indexOfJoinColumnInTable,
                                false);
                    } else {
                        joinedRow = RowUtils
                            .constructNewRow(newHeader, rowA, rowB, indexOfJoinColumnInTable, true);
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
            int anotherRowSize = headerB.hasKey() ? rowsB.get(0).getValues().length + 1
                : rowsB.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsA.size(); i++) {
                if (!bitmapA.get(i)) {
                    Row unmatchedRow = RowUtils
                        .constructUnmatchedRow(newHeader, rowsA.get(i), anotherRowSize, true);
                    transformedRows.add(unmatchedRow);
                }
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = headerA.hasKey() ? rowsA.get(0).getValues().length + 1
                : rowsA.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsB.size(); i++) {
                if (!bitmapB.get(i)) {
                    Row unmatchedRow = RowUtils
                        .constructUnmatchedRow(newHeader, rowsB.get(i), anotherRowSize, false);
                    transformedRows.add(unmatchedRow);
                }
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeHashOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB)
        throws PhysicalException {
        OuterJoinType outerType = outerJoin.getOuterJoinType();
        Filter filter = outerJoin.getFilter();

        List<String> joinColumns = new ArrayList<>(outerJoin.getJoinColumns());
        if (outerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, tableA.getHeader(), tableB.getHeader(),
                outerJoin.getPrefixA(), outerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns
            .isEmpty())) {
            throw new InvalidOperatorParameterException(
                "using(or natural) and on operator cannot be used at the same time");
        }

        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();

        String joinColumnA, joinColumnB;
        if (filter != null) {
            if (!filter.getType().equals(FilterType.Path)) {
                throw new InvalidOperatorParameterException(
                    "hash join only support one path filter yet.");
            }
            Pair<String, String> p = FilterUtils.getJoinColumnFromPathFilter((PathFilter) filter);
            if (p == null) {
                throw new InvalidOperatorParameterException(
                    "hash join only support equal path filter yet.");
            }
            if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                joinColumnA = p.k.replaceFirst(outerJoin.getPrefixA() + '.', "");
                joinColumnB = p.v.replaceFirst(outerJoin.getPrefixB() + ".", "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                joinColumnA = p.v.replaceFirst(outerJoin.getPrefixA() + '.', "");
                joinColumnB = p.k.replaceFirst(outerJoin.getPrefixB() + ".", "");
            } else {
                throw new InvalidOperatorParameterException("invalid hash join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException(
                    "hash join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(outerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1
                && headerB.indexOf(outerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                joinColumnA = joinColumnB = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid hash join column input.");
            }
        }

        boolean needTypeCast = false;
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
        if (!rowsA.isEmpty() && !rowsB.isEmpty()) {
            Value valueA = rowsA.get(0).getAsValue(outerJoin.getPrefixA() + '.' + joinColumnA);
            Value valueB = rowsB.get(0).getAsValue(outerJoin.getPrefixB() + '.' + joinColumnB);
            if (valueA.getDataType() != valueB.getDataType()) {
                if (ValueUtils.isNumericType(valueA) && ValueUtils.isNumericType(valueB)) {
                    needTypeCast = true;
                }
            }
        }

        Bitmap bitmapA = new Bitmap(rowsA.size());
        Bitmap bitmapB = new Bitmap(rowsB.size());

        HashMap<Integer, List<Row>> rowsBHashMap = new HashMap<>();
        HashMap<Integer, List<Integer>> indexOfRowBHashMap = new HashMap<>();
        for (int indexB = 0; indexB < rowsB.size(); indexB++) {
            Value value = rowsB.get(indexB).getAsValue(outerJoin.getPrefixB() + '.' + joinColumnB);
            if (value == null) {
                continue;
            }
            if (needTypeCast) {
                value = ValueUtils.transformToDouble(value);
            }
            int hash;
            if (value.getDataType() == DataType.BINARY) {
                hash = Arrays.hashCode(value.getBinaryV());
            } else {
                hash = value.getValue().hashCode();
            }
            List<Row> l =
                rowsBHashMap.containsKey(hash) ? rowsBHashMap.get(hash) : new ArrayList<>();
            List<Integer> il =
                rowsBHashMap.containsKey(hash) ? indexOfRowBHashMap.get(hash) : new ArrayList<>();
            l.add(rowsB.get(indexB));
            il.add(indexB);
            rowsBHashMap.put(hash, l);
            indexOfRowBHashMap.put(hash, il);
        }

        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) { // Join condition: on
            newHeader = RowUtils
                .constructNewHead(headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());
            for (int indexA = 0; indexA < rowsA.size(); indexA++) {
                Row rowA = rowsA.get(indexA);
                Value value = rowA.getAsValue(outerJoin.getPrefixA() + '.' + joinColumnA);
                if (value == null) {
                    continue;
                }
                if (needTypeCast) {
                    value = ValueUtils.transformToDouble(value);
                }
                int hash;
                if (value.getDataType() == DataType.BINARY) {
                    hash = Arrays.hashCode(value.getBinaryV());
                } else {
                    hash = value.getValue().hashCode();
                }

                if (rowsBHashMap.containsKey(hash)) {
                    List<Row> hashRowsB = rowsBHashMap.get(hash);
                    List<Integer> hashIndexB = indexOfRowBHashMap.get(hash);
                    for (int i = 0; i < hashRowsB.size(); i++) {
                        Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, hashRowsB.get(i));
                        int indexB = hashIndexB.get(i);
                        if (FilterUtils.validate(filter, joinedRow)) {
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
            }
        } else { // Join condition: natural or using
            Pair<int[], Header> pair;
            if (outerType == OuterJoinType.RIGHT) {
                pair = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), Collections.singletonList(joinColumnA), false);
            } else {
                pair = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), Collections.singletonList(joinColumnB), true);
            }
            int[] indexOfJoinColumnInTable = pair.getK();
            newHeader = pair.getV();

            for (int indexA = 0; indexA < rowsA.size(); indexA++) {
                Row rowA = rowsA.get(indexA);
                Value value = rowA.getAsValue(outerJoin.getPrefixA() + '.' + joinColumnA);
                if (value == null) {
                    continue;
                }
                if (needTypeCast) {
                    value = ValueUtils.transformToDouble(value);
                }
                int hash;
                if (value.getDataType() == DataType.BINARY) {
                    hash = Arrays.hashCode(value.getBinaryV());
                } else {
                    hash = value.getValue().hashCode();
                }

                if (rowsBHashMap.containsKey(hash)) {
                    List<Row> hashRowsB = rowsBHashMap.get(hash);
                    List<Integer> hashIndexB = indexOfRowBHashMap.get(hash);
                    for (int i = 0; i < hashRowsB.size(); i++) {
                        Row joinedRow;
                        if (outerType == OuterJoinType.RIGHT) {
                            joinedRow = RowUtils.constructNewRow(newHeader, rowA, hashRowsB.get(i),
                                indexOfJoinColumnInTable, false);
                        } else {
                            joinedRow = RowUtils.constructNewRow(newHeader, rowA, hashRowsB.get(i),
                                indexOfJoinColumnInTable, true);
                        }

                        int indexB = hashIndexB.get(i);
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
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
            int anotherRowSize = headerB.hasKey() ? rowsB.get(0).getValues().length + 1
                : rowsB.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsA.size(); i++) {
                if (!bitmapA.get(i)) {
                    Row unMatchedRow = RowUtils
                        .constructUnmatchedRow(newHeader, rowsA.get(i), anotherRowSize, true);
                    transformedRows.add(unMatchedRow);
                }
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = headerA.hasKey() ? rowsA.get(0).getValues().length + 1
                : rowsA.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsB.size(); i++) {
                if (!bitmapB.get(i)) {
                    Row unMatchedRow = RowUtils
                        .constructUnmatchedRow(newHeader, rowsB.get(i), anotherRowSize, false);
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
                    String joinColumnA = fieldA.getName()
                        .replaceFirst(outerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName()
                        .replaceFirst(outerJoin.getPrefixB() + '.', "");
                    if (joinColumnA.equals(joinColumnB)) {
                        joinColumns.add(joinColumnA);
                    }
                }
            }
            if (joinColumns.isEmpty()) {
                throw new PhysicalException("natural join has no matching columns");
            }
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns
            .isEmpty())) {
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
            throw new InvalidOperatorParameterException(
                "input rows in merge join haven't be sorted.");
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
            newHeader = RowUtils
                .constructNewHead(headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());
            int indexA = 0;
            int indexB = 0;
            int startIndexOfContinuousEqualValuesB = 0;
            while (indexA < rowsA.size() && indexB < rowsB.size()) {
                int flagAEqualB = RowUtils
                    .compareRowsSortedByColumns(rowsA.get(indexA), rowsB.get(indexB),
                        outerJoin.getPrefixA(), outerJoin.getPrefixB(), joinColumnsA, joinColumnsB);
                if (flagAEqualB == 0) {
                    Row joinedRow = RowUtils
                        .constructNewRow(newHeader, rowsA.get(indexA), rowsB.get(indexB));
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
                            int flagAEqualNextA = RowUtils
                                .compareRowsSortedByColumns(rowsA.get(indexA),
                                    rowsA.get(indexA + 1), outerJoin.getPrefixA(),
                                    outerJoin.getPrefixA(), joinColumnsA, joinColumnsA);
                            int flagBEqualNextB = RowUtils
                                .compareRowsSortedByColumns(rowsB.get(indexB),
                                    rowsB.get(indexB + 1), outerJoin.getPrefixB(),
                                    outerJoin.getPrefixB(), joinColumnsB, joinColumnsB);
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
                pair = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), joinColumns, false);
            } else {
                pair = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), joinColumns, true);
            }
            int[] indexOfJoinColumnInTable = pair.getK();
            newHeader = pair.getV();
            int indexA = 0;
            int indexB = 0;
            int startIndexOfContinuousEqualValuesB = 0;
            while (indexA < rowsA.size() && indexB < rowsB.size()) {
                int flagAEqualB = RowUtils
                    .compareRowsSortedByColumns(rowsA.get(indexA), rowsB.get(indexB),
                        outerJoin.getPrefixA(), outerJoin.getPrefixB(), joinColumnsA, joinColumnsB);
                if (flagAEqualB == 0) {
                    Row joinedRow;
                    if (outerType == OuterJoinType.RIGHT) {
                        joinedRow = RowUtils
                            .constructNewRow(newHeader, rowsA.get(indexA), rowsB.get(indexB),
                                indexOfJoinColumnInTable, false);
                    } else {
                        joinedRow = RowUtils
                            .constructNewRow(newHeader, rowsA.get(indexA), rowsB.get(indexB),
                                indexOfJoinColumnInTable, true);
                    }

                    if (!bitmapA.get(indexA)) {
                        bitmapA.mark(indexA);
                    }
                    if (!bitmapB.get(indexB)) {
                        bitmapB.mark(indexB);
                    }
                    transformedRows.add(joinedRow);

                    int flagAEqualNextA = RowUtils
                        .compareRowsSortedByColumns(rowsA.get(indexA), rowsA.get(indexA + 1),
                            outerJoin.getPrefixA(), outerJoin.getPrefixA(), joinColumnsA,
                            joinColumnsA);
                    int flagBEqualNextB = RowUtils
                        .compareRowsSortedByColumns(rowsB.get(indexB), rowsB.get(indexB + 1),
                            outerJoin.getPrefixB(), outerJoin.getPrefixB(), joinColumnsB,
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
            int anotherRowSize = headerB.hasKey() ? rowsB.get(0).getValues().length + 1
                : rowsB.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsA.size(); i++) {
                if (!bitmapA.get(i)) {
                    Row unMatchedRow = RowUtils
                        .constructUnmatchedRow(newHeader, rowsA.get(i), anotherRowSize, true);
                    transformedRows.add(unMatchedRow);
                }
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = headerA.hasKey() ? rowsA.get(0).getValues().length + 1
                : rowsA.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsB.size(); i++) {
                if (!bitmapB.get(i)) {
                    Row unMatchedRow = RowUtils
                        .constructUnmatchedRow(newHeader, rowsB.get(i), anotherRowSize, false);
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
        Header newHeader = RowUtils.constructNewHead(tableA.getHeader(), tableB.getHeader(), true);
    
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
    
        List<Row> transformedRows = new ArrayList<>();
        Filter filter = singleJoin.getFilter();
        boolean matched;
        int anotherRowSize = tableB.getHeader().getFieldSize();
        for (Row rowA : rowsA) {
            matched = false;
            for (Row rowB : rowsB) {
                Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, rowB, true);
                if (FilterUtils.validate(filter, joinedRow)) {
                    if (!matched) {
                        matched = true;
                        transformedRows.add(joinedRow);
                    } else {
                        throw new PhysicalException("the return value of sub-query has more than one rows");
                    }
                }
            }
            if (!matched) {
                Row unmatchedRow = RowUtils.constructUnmatchedRow(newHeader, rowA, anotherRowSize, true);
                transformedRows.add(unmatchedRow);
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeHashSingleJoin(SingleJoin singleJoin, Table tableA, Table tableB)
            throws PhysicalException {
        Header newHeader = RowUtils.constructNewHead(tableA.getHeader(), tableB.getHeader(), true);
        Pair<String, String> joinPath = getJoinPathFromFilter(singleJoin.getFilter(), tableA.getHeader(), tableB.getHeader());
        String joinPathA = joinPath.k;
        String joinPathB = joinPath.v;

        boolean needTypeCast = false;
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
        if (!rowsA.isEmpty() && !rowsB.isEmpty()) {
            Value valueA = rowsA.get(0).getAsValue(joinPathA);
            Value valueB = rowsB.get(0).getAsValue(joinPathB);
            if (valueA.getDataType() != valueB.getDataType()) {
                if (ValueUtils.isNumericType(valueA) && ValueUtils.isNumericType(valueB)) {
                    needTypeCast = true;
                }
            }
        }

        HashMap<Integer, List<Row>> rowsBHashMap = new HashMap<>();
        for (Row rowB: rowsB) {
            Value value = rowB.getAsValue(joinPathB);
            if (value == null) {
                continue;
            }
            if (needTypeCast) {
                value = ValueUtils.transformToDouble(value);
            }
            int hash;
            if (value.getDataType() == DataType.BINARY) {
                hash = Arrays.hashCode(value.getBinaryV());
            } else {
                hash = value.getValue().hashCode();
            }
            List<Row> l = rowsBHashMap.containsKey(hash) ? rowsBHashMap.get(hash) : new ArrayList<>();
            l.add(rowB);
            rowsBHashMap.put(hash, l);
        }

        List<Row> transformedRows = new ArrayList<>();
        int anotherRowSize = tableB.getHeader().getFieldSize();
        for (Row rowA : rowsA) {
            Value value = rowA.getAsValue(joinPathA);
            if (value == null) {
                continue;
            }
            if (needTypeCast) {
                value = ValueUtils.transformToDouble(value);
            }
            int hash;
            if (value.getDataType() == DataType.BINARY) {
                hash = Arrays.hashCode(value.getBinaryV());
            } else {
                hash = value.getValue().hashCode();
            }

            if (rowsBHashMap.containsKey(hash)) {
                List<Row> hashRowsB = rowsBHashMap.get(hash);
                if (hashRowsB.size() == 1) {
                    Row joinedRow = RowUtils.constructNewRow(newHeader, rowA, hashRowsB.get(0), true);
                    transformedRows.add(joinedRow);
                } else {
                    throw new PhysicalException("the return value of sub-query has more than one rows");
                }
            } else {
                Row unmatchedRow = RowUtils.constructUnmatchedRow(newHeader, rowA, anotherRowSize, true);
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
        Header targetHeader = constructNewHead(tableA.getHeader(), markJoin.getMarkColumn());
        Header joinHeader = RowUtils.constructNewHead(tableA.getHeader(), tableB.getHeader(), true);;

        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();

        List<Row> transformedRows = new ArrayList<>();
        Filter filter = markJoin.getFilter();
        boolean matched;
        for (Row rowA: rowsA) {
            matched = false;
            for (Row rowB: rowsB) {
                Row joinedRow = RowUtils.constructNewRow(joinHeader, rowA, rowB, true);
                if (FilterUtils.validate(filter, joinedRow)) {
                    matched = true;
                    Row returnRow = RowUtils.constructNewRowWithMark(targetHeader, rowA, !markJoin.isAntiJoin());
                    transformedRows.add(returnRow);
                    break;
                }
            }
            if (!matched) {
                Row unmatchedRow = RowUtils.constructNewRowWithMark(targetHeader, rowA, markJoin.isAntiJoin());
                transformedRows.add(unmatchedRow);
            }
        }
        return new Table(targetHeader, transformedRows);
    }

    private RowStream executeHashMarkJoin(MarkJoin markJoin, Table tableA, Table tableB)
            throws PhysicalException {
        Header newHeader = constructNewHead(tableA.getHeader(), markJoin.getMarkColumn());
        Pair<String, String> joinPath = getJoinPathFromFilter(markJoin.getFilter(), tableA.getHeader(), tableB.getHeader());
        String joinPathA = joinPath.k;
        String joinPathB = joinPath.v;

        boolean needTypeCast = false;
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
        if (!rowsA.isEmpty() && !rowsB.isEmpty()) {
            Value valueA = rowsA.get(0).getAsValue(joinPathA);
            Value valueB = rowsB.get(0).getAsValue(joinPathB);
            if (valueA.getDataType() != valueB.getDataType()) {
                if (ValueUtils.isNumericType(valueA) && ValueUtils.isNumericType(valueB)) {
                    needTypeCast = true;
                }
            }
        }

        HashMap<Integer, List<Row>> rowsBHashMap = new HashMap<>();
        for (Row rowB: rowsB) {
            Value value = rowB.getAsValue(joinPathB);
            if (value == null) {
                continue;
            }
            if (needTypeCast) {
                value = ValueUtils.transformToDouble(value);
            }
            int hash;
            if (value.getDataType() == DataType.BINARY) {
                hash = Arrays.hashCode(value.getBinaryV());
            } else {
                hash = value.getValue().hashCode();
            }
            List<Row> l = rowsBHashMap.containsKey(hash) ? rowsBHashMap.get(hash) : new ArrayList<>();
            l.add(rowB);
            rowsBHashMap.put(hash, l);
        }

        List<Row> transformedRows = new ArrayList<>();
        for (Row rowA : rowsA) {
            Value value = rowA.getAsValue(joinPathA);
            if (value == null) {
                continue;
            }
            if (needTypeCast) {
                value = ValueUtils.transformToDouble(value);
            }
            int hash;
            if (value.getDataType() == DataType.BINARY) {
                hash = Arrays.hashCode(value.getBinaryV());
            } else {
                hash = value.getValue().hashCode();
            }

            if (rowsBHashMap.containsKey(hash)) {
                Row returnRow = RowUtils.constructNewRowWithMark(newHeader, rowA, !markJoin.isAntiJoin());
                transformedRows.add(returnRow);
            } else {
                Row unmatchedRow = RowUtils.constructNewRowWithMark(newHeader, rowA, markJoin.isAntiJoin());
                transformedRows.add(unmatchedRow);
            }
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
                "join operator is not support for field " + join.getJoinBy() + " except for "
                    + Constants.KEY
                    + " and " + Constants.ORDINAL);
        }
    }

    private RowStream executeUnion(Union union, Table tableA, Table tableB)
        throws PhysicalException {
        // 检查时间是否一致
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
        if (headerA.hasKey() ^ headerB.hasKey()) {
            throw new InvalidOperatorParameterException(
                "row stream to be union must have same fields");
        }
        boolean hasTimestamp = headerA.hasKey();
        Set<Field> targetFieldSet = new HashSet<>();
        targetFieldSet.addAll(headerA.getFields());
        targetFieldSet.addAll(headerB.getFields());
        List<Field> targetFields = new ArrayList<>(targetFieldSet);
        Header targetHeader;
        List<Row> rows = new ArrayList<>();
        if (!hasTimestamp) {
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

    private static class NaiveOperatorMemoryExecutorHolder {

        private static final NaiveOperatorMemoryExecutor INSTANCE = new NaiveOperatorMemoryExecutor();

        private NaiveOperatorMemoryExecutorHolder() {
        }

    }

}
