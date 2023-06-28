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

import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.Objects;

public final class ColumnsInterval implements Comparable<ColumnsInterval> {

  private String startColumn;

  private String endColumn;

  private String schemaPrefix = null;

  private final String START_FORMAT = "%s" + PathUtils.MIN_CHAR;
  private final String END_FORMAT = "%s" + PathUtils.MAX_CHAR;

  public ColumnsInterval(String startColumn, String endColumn) {
    this.startColumn = startColumn;
    this.endColumn = endColumn;
  }

  public ColumnsInterval(String column) {
    this.startColumn = String.format(START_FORMAT, column);
    this.endColumn = String.format(END_FORMAT, column);
  }

  private String realColumn(String column) {
    if (column != null && schemaPrefix != null) {
      return schemaPrefix + "." + column;
    }
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

  @Override
  public String toString() {
    return startColumn + "-" + endColumn;
  }

  public String getSchemaPrefix() {
    return schemaPrefix;
  }

  public void setSchemaPrefix(String schemaPrefix) {
    this.schemaPrefix = schemaPrefix;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnsInterval that = (ColumnsInterval) o;
    return Objects.equals(startColumn, that.getStartColumn())
        && Objects.equals(endColumn, that.getEndColumn());
  }

  @Override
  public int hashCode() {
    return Objects.hash(startColumn, endColumn);
  }

  public boolean isContain(String columnName) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(startColumn);
    String realEndColumn = realColumn(endColumn);

    return (realStartColumn == null
            || (columnName != null && StringUtils.compare(columnName, realStartColumn, true) >= 0))
        && (realEndColumn == null
            || (columnName != null && StringUtils.compare(columnName, realEndColumn, false) < 0));
  }

  public boolean isCompletelyBefore(String columnName) {
    // judge if is the dummy node && it will have specific prefix
    String realEndColumn = realColumn(endColumn);

    return realEndColumn != null && columnName != null && realEndColumn.compareTo(columnName) <= 0;
  }

  public boolean isIntersect(ColumnsInterval columnsInterval) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(this.startColumn);
    String realEndColumn = realColumn(this.endColumn);

    return (columnsInterval.getStartColumn() == null
            || realEndColumn == null
            || StringUtils.compare(columnsInterval.getStartColumn(), realEndColumn, false) < 0)
        && (columnsInterval.getEndColumn() == null
            || realStartColumn == null
            || StringUtils.compare(columnsInterval.getEndColumn(), realStartColumn, true) >= 0);
  }

  public ColumnsInterval getIntersect(ColumnsInterval columnsInterval) {
    if (!isIntersect(columnsInterval)) {
      return null;
    }
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(startColumn);
    String realEndColumn = realColumn(endColumn);

    String start =
        realStartColumn == null
            ? columnsInterval.getStartColumn()
            : columnsInterval.getStartColumn() == null
                ? realStartColumn
                : StringUtils.compare(columnsInterval.getStartColumn(), realStartColumn, true) < 0
                    ? realStartColumn
                    : columnsInterval.getStartColumn();
    String end =
        realEndColumn == null
            ? columnsInterval.getEndColumn()
            : columnsInterval.getEndColumn() == null
                ? realEndColumn
                : StringUtils.compare(columnsInterval.getEndColumn(), realEndColumn, false) < 0
                    ? columnsInterval.getEndColumn()
                    : realEndColumn;
    return new ColumnsInterval(start, end);
  }

  public boolean isCompletelyAfter(ColumnsInterval columnsInterval) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(startColumn);

    return columnsInterval.getEndColumn() != null
        && realStartColumn != null
        && StringUtils.compare(columnsInterval.getEndColumn(), realStartColumn, true) < 0;
  }

  public boolean isAfter(String colName) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(startColumn);

    return realStartColumn != null && StringUtils.compare(colName, realStartColumn, true) < 0;
  }

  @Override
  public int compareTo(ColumnsInterval o) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(startColumn);
    String realEndColumn = realColumn(endColumn);

    int value = compareTo(realStartColumn, o.getStartColumn());
    if (value != 0) {
      return value;
    }
    return compareTo(realEndColumn, o.getEndColumn());
  }

  private static int compareTo(String s1, String s2) {
    if (s1 == null && s2 == null) {
      return 0;
    }
    if (s1 == null) {
      return -1;
    }
    if (s2 == null) {
      return 1;
    }
    return s1.compareTo(s2);
  }
}
