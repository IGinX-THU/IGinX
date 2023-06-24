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

import static cn.edu.tsinghua.iginx.utils.StringUtils.isContainSpecialChar;

import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ColumnsRange implements Comparable<ColumnsRange> {

    public static final Logger logger = LoggerFactory.getLogger(ColumnsRange.class);

    private String startColumn;

    private String endColumn;

    private String schemaPrefix = null;

    // 右边界是否为闭
    private boolean isClosed;

    public ColumnsRange(String startColumn, String endColumn, boolean isClosed) {
        this.startColumn = startColumn;
        this.endColumn = endColumn;
        this.isClosed = isClosed;
    }

    public ColumnsRange(String column, boolean isClosed) {
        this.startColumn = column;
        this.endColumn = column;
        this.isClosed = isClosed;
    }

    public ColumnsRange(String startColumn, String endColumn) {
        this(startColumn, endColumn, false);
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

    public String getStartColumn() {
        return startColumn;
    }

    public void setStartColumn(String startColumn) {
        this.startColumn = startColumn;
    }

    public String getEndColumn() {
        return endColumn;
    }

    public void setEndColumn(String endColumn) {
        this.endColumn = endColumn;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void setClosed(boolean closed) {
        isClosed = closed;
    }

    @Override
    public String toString() {
        return "" + startColumn + "-" + endColumn;
    }

    public String getSchemaPrefix() {
        return schemaPrefix;
    }

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

    public boolean isContain(String colName) {
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);
        String endTimeSeries = realColumn(this.endColumn);
        boolean isContainBeg =
                colName != null && StringUtils.compare(colName, startTimeSeries, true) >= 0;
        boolean isContainEnd = false;
        if (colName != null) {
            int res = StringUtils.compare(colName, endTimeSeries, false);
            isContainEnd = isClosed ? res <= 0 : res < 0;
        }

        return (startTimeSeries == null || isContainBeg) && (endTimeSeries == null || isContainEnd);
    }

    public boolean isCompletelyBefore(String colName) {
        // judge if is the dummy node && it will have specific prefix
        String endTimeSeries = realColumn(this.endColumn);

        return endTimeSeries != null && colName != null && endTimeSeries.compareTo(colName) <= 0;
    }

    public boolean isIntersect(ColumnsRange colRange) {
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);
        String endTimeSeries = realColumn(this.endColumn);
        if (colRange.getStartColumn() == null
                || endTimeSeries == null && colRange.getEndColumn() == null
                || startTimeSeries == null) {
            return true;
        }
        boolean isContainEnd =
                colRange.getEndColumn() == null
                        || StringUtils.compare(colRange.getEndColumn(), startTimeSeries, true) >= 0;
        boolean isContainBeg;
        if (colRange.getStartColumn() != null) {
            int res = StringUtils.compare(colRange.getStartColumn(), endTimeSeries, false);
            isContainBeg = isClosed ? res <= 0 : res < 0;
        } else {
            isContainBeg = true;
        }

        return isContainBeg && isContainEnd;
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
        return new ColumnsRange(start, end);
    }

    public boolean isCompletelyAfter(ColumnsRange colRange) {
        // judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realColumn(this.startColumn);
        if (colRange.getEndColumn() == null || startTimeSeries != null) {
            return false;
        }
        int isAfter = StringUtils.compare(colRange.getEndColumn(), startTimeSeries, true);

        return isAfter < 0;
    }

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

    // Strange function: it should not work on the implementation of TimeSeriesPrefixRange
    public static ColumnsRange fromString(String str) throws IllegalArgumentException {
        if (str.contains("-") && !isContainSpecialChar(str)) {
            String[] parts = str.split("-");
            if (parts.length != 2) {
                logger.error("Input string {} in invalid format of ColumnsRange ", str);
                throw new IllegalArgumentException("Input invalid string format in ColumnsRange");
            }
            return new ColumnsRange(
                    parts[0].equals("null") ? null : parts[0],
                    parts[1].equals("null") ? null : parts[1]);
        } else {
            if (str.contains(".*") && str.indexOf(".*") == str.length() - 2)
                str = str.substring(0, str.length() - 2);
            if (!isContainSpecialChar(str)) return new ColumnsRange(str, true);
            else {
                logger.error("Input string {} in invalid format of ColumnsPrefixRange ", str);
                throw new IllegalArgumentException("Input invalid string format in ColumnsRange");
            }
        }
    }
}
