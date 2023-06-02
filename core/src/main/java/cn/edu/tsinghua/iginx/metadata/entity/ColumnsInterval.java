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
package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.alibaba.fastjson2.annotation.JSONType;
import java.util.Objects;

@JSONType(typeName = "ColumnsInterval")
public final class ColumnsInterval implements ColumnsRange {

    private final ColumnsRange.TYPE type = ColumnsRange.TYPE.NORMAL;

    private String startColumn;

    private String endColumn;

    private String schemaPrefix = null;

    // 右边界是否为闭
    private boolean isClosed;

    public ColumnsInterval(String startColumn, String endColumn, boolean isClosed) {
        this.startColumn = startColumn;
        this.endColumn = endColumn;
        this.isClosed = isClosed;
    }

    public ColumnsInterval(String startColumn, String endColumn) {
        this(startColumn, endColumn, false);
    }

    public static ColumnsRange fromString(String str) {
        String[] parts = str.split("-");
        assert parts.length == 2;
        return new ColumnsInterval(
                parts[0].equals("null") ? null : parts[0],
                parts[1].equals("null") ? null : parts[1]);
    }

    private static int compareTo(String s1, String s2) {
        if (s1 == null && s2 == null) return 0;
        if (s1 == null) return -1;
        if (s2 == null) return 1;
        return s1.compareTo(s2);
    }

    private String realColumn(String column) {
        if (column != null && schemaPrefix != null) return schemaPrefix + "." + column;
        return column;
    }

    @Override
    public TYPE getType() {
        return type;
    }

    @Override
    public String getStartColumn() {
        return startColumn;
    }

    @Override
    public void setStartColumn(String startColumn) {
        this.startColumn = startColumn;
    }

    @Override
    public String getEndColumn() {
        return endColumn;
    }

    @Override
    public void setEndColumn(String endColumn) {
        this.endColumn = endColumn;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void setClosed(boolean closed) {
        isClosed = closed;
    }

    @Override
    public String toString() {
        return "" + startColumn + "-" + endColumn;
    }

    @Override
    public String getSchemaPrefix() {
        return schemaPrefix;
    }

    @Override
    public void setSchemaPrefix(String schemaPrefix) {
        this.schemaPrefix = schemaPrefix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnsRange that = (ColumnsRange) o;
        return Objects.equals(startColumn, that.getStartColumn())
                && Objects.equals(endColumn, that.getEndColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(startColumn, endColumn);
    }

    @Override
    public boolean isContain(String colName) {
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);
        String endTimeSeries = realColumn(this.endColumn);

        return (startTimeSeries == null
                        || (colName != null
                                && StringUtils.compare(colName, startTimeSeries, true) >= 0))
                && (endTimeSeries == null
                        || (colName != null
                                && StringUtils.compare(colName, endTimeSeries, false) < 0));
    }

    public boolean isCompletelyBefore(String colName) {
        // judge if is the dummy node && it will have specific prefix
        String endTimeSeries = realColumn(this.endColumn);

        return endTimeSeries != null && colName != null && endTimeSeries.compareTo(colName) <= 0;
    }

    @Override
    public boolean isIntersect(ColumnsRange colRange) {
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);
        String endTimeSeries = realColumn(this.endColumn);

        return (colRange.getStartColumn() == null
                        || endTimeSeries == null
                        || StringUtils.compare(colRange.getStartColumn(), endTimeSeries, false) < 0)
                && (colRange.getEndColumn() == null
                        || startTimeSeries == null
                        || StringUtils.compare(colRange.getEndColumn(), startTimeSeries, true)
                                >= 0);
    }

    public ColumnsRange getIntersect(ColumnsRange colRange) {
        if (!isIntersect(colRange)) {
            return null;
        }
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);
        String endTimeSeries = realColumn(this.endColumn);

        String start =
                startTimeSeries == null
                        ? colRange.getStartColumn()
                        : colRange.getStartColumn() == null
                                ? startTimeSeries
                                : StringUtils.compare(
                                                        colRange.getStartColumn(),
                                                        startTimeSeries,
                                                        true)
                                                < 0
                                        ? startTimeSeries
                                        : colRange.getStartColumn();
        String end =
                endTimeSeries == null
                        ? colRange.getEndColumn()
                        : colRange.getEndColumn() == null
                                ? endTimeSeries
                                : StringUtils.compare(colRange.getEndColumn(), endTimeSeries, false)
                                                < 0
                                        ? colRange.getEndColumn()
                                        : endTimeSeries;
        return new ColumnsInterval(start, end);
    }

    @Override
    public boolean isCompletelyAfter(ColumnsRange colRange) {
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);

        return colRange.getEndColumn() != null
                && startTimeSeries != null
                && StringUtils.compare(colRange.getEndColumn(), startTimeSeries, true) < 0;
    }

    @Override
    public boolean isAfter(String colName) {
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);

        return startTimeSeries != null && StringUtils.compare(colName, startTimeSeries, true) < 0;
    }

    @Override
    public int compareTo(ColumnsRange o) {
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);
        String endTimeSeries = realColumn(this.endColumn);

        int value = compareTo(startTimeSeries, o.getStartColumn());
        if (value != 0) return value;
        return compareTo(endTimeSeries, o.getEndColumn());
    }
}
