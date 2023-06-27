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

import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
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

  private final String START_FORMAT = "%s" + PathUtils.MIN_CHAR;
  private final String END_FORMAT = "%s" + PathUtils.MAX_CHAR;

  public ColumnsRange(String startColumn, String endColumn, boolean isClosed) {
    this.startColumn = startColumn;
    this.endColumn = endColumn;
    this.isClosed = isClosed;
  }

  public ColumnsRange(String column) {
    this.startColumn = String.format(START_FORMAT, column);
    this.endColumn = String.format(END_FORMAT, column);
    this.isClosed = true;
  }

  public ColumnsRange(String startColumn, String endColumn) {
    this(startColumn, endColumn, false);
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

  private String realColumn(String column) {
    if (column != null && schemaPrefix != null) {
      return this.schemaPrefix + "." + column;
    }
    return column;
  }

  public String getStartColumn() {
    return this.startColumn;
  }

  public void setStartColumn(String startColumn) {
    this.startColumn = startColumn;
  }

  public String getEndColumn() {
    return this.endColumn;
  }

  public void setEndColumn(String endColumn) {
    this.endColumn = endColumn;
  }

  public boolean isClosed() {
    return this.isClosed;
  }

  public void setClosed(boolean closed) {
    this.isClosed = closed;
  }

  @Override
  public String toString() {
    return "" + this.startColumn + "-" + this.endColumn;
  }

  public String getSchemaPrefix() {
    return this.schemaPrefix;
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
    ColumnsRange that = (ColumnsRange) o;
    return Objects.equals(this.startColumn, that.getStartColumn())
        && Objects.equals(this.endColumn, that.getEndColumn());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.startColumn, this.endColumn);
  }

  public boolean isContain(String colName) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(this.startColumn);
    String realEndColumn = realColumn(this.endColumn);
    boolean isContainBeg = false;
    boolean isContainEnd = false;
    if (colName != null) {
      int res = StringUtils.compare(colName, realEndColumn, false);
      isContainBeg = StringUtils.compare(colName, realStartColumn, true) >= 0;
      isContainEnd = this.isClosed ? res <= 0 : res < 0;
    }

    return (realStartColumn == null || isContainBeg) && (realEndColumn == null || isContainEnd);
  }

  public boolean isIntersect(ColumnsRange colRange) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(this.startColumn);
    String realEndColumn = realColumn(this.endColumn);
    boolean isContainEnd = true;
    boolean isContainBeg = true;
    if (colRange.getEndColumn() != null && realStartColumn != null) {
      int res = StringUtils.compare(colRange.getEndColumn(), realStartColumn, true);
      isContainEnd = colRange.isClosed ? res >= 0 : res > 0;
    }
    if (colRange.getStartColumn() != null && realEndColumn != null) {
      int res = StringUtils.compare(colRange.getStartColumn(), realEndColumn, false);
      isContainBeg = this.isClosed ? res <= 0 : res < 0;
    }

    return isContainBeg && isContainEnd;
  }

  // TODO: 如果后续有需要，根据isClosed变量进行修改

  //  public ColumnsRange getIntersect(ColumnsRange colRange) {
  //    if (!isIntersect(colRange)) {
  //      return null;
  //    }
  //    // judge if is the dummy node && it will have specific prefix
  //    String realStartColumn = realColumn(this.startColumn);
  //    String realEndColumn = realColumn(this.endColumn);
  //
  //    String start =
  //        realStartColumn == null
  //            ? colRange.getStartColumn()
  //            : colRange.getStartColumn() == null
  //                ? realStartColumn
  //                : StringUtils.compare(colRange.getStartColumn(), realStartColumn, true) < 0
  //                    ? realStartColumn
  //                    : colRange.getStartColumn();
  //    String end =
  //        realEndColumn == null
  //            ? colRange.getEndColumn()
  //            : colRange.getEndColumn() == null
  //                ? realEndColumn
  //                : StringUtils.compare(colRange.getEndColumn(), realEndColumn, false) < 0
  //                    ? colRange.getEndColumn()
  //                    : realEndColumn;
  //    return new ColumnsRange(start, end);
  //  }

  public boolean isCompletelyAfter(ColumnsRange colRange) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(this.startColumn);
    if (colRange.getEndColumn() == null || realStartColumn == null) {
      return false;
    }
    int res = StringUtils.compare(colRange.getEndColumn(), realStartColumn, true);
    return colRange.isClosed ? res < 0 : res <= 0;
  }

  public boolean isAfter(String colName) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(this.startColumn);

    return realStartColumn != null && StringUtils.compare(colName, realStartColumn, true) < 0;
  }

  @Override
  public int compareTo(ColumnsRange o) {
    // judge if is the dummy node && it will have specific prefix
    String realStartColumn = realColumn(this.startColumn);
    String realEndColumn = realColumn(this.endColumn);

    int value = compareTo(realStartColumn, o.getStartColumn());
    if (value != 0) {
      return value;
    }
    return compareTo(realEndColumn, o.getEndColumn());
  }

  // Strange function: it should not work on the implementation of ColumnsPrefixRange
  public static ColumnsRange fromString(String str) throws IllegalArgumentException {
    if (str.contains("-") && !isContainSpecialChar(str)) {
      String[] parts = str.split("-");
      if (parts.length != 2) {
        logger.error("Input string {} in invalid format of ColumnsRange ", str);
        throw new IllegalArgumentException("Input invalid string format in ColumnsRange");
      }
      return new ColumnsRange(
          parts[0].equals("null") ? null : parts[0], parts[1].equals("null") ? null : parts[1]);
    } else {
      if (str.contains(".*") && str.indexOf(".*") == str.length() - 2) {
        str = str.substring(0, str.length() - 2);
      }
      if (!isContainSpecialChar(str)) {
        return new ColumnsRange(str);
      } else {
        logger.error("Input string {} in invalid format of ColumnsPrefixRange ", str);
        throw new IllegalArgumentException("Input invalid string format in ColumnsRange");
      }
    }
  }
}
