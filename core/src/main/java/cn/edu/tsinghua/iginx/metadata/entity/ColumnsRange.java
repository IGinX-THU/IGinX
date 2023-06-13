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

import com.alibaba.fastjson2.annotation.JSONType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JSONType(seeAlso = {ColumnsInterval.class, ColumnsPrefixRange.class})
public interface ColumnsRange extends Comparable<ColumnsRange> {

    public static Logger logger = LoggerFactory.getLogger(ColumnsRange.class);

    public static enum RangeType {
        PREFIX,
        NORMAL
    }

    public RangeType getRangeType();

    public default boolean isNormal() {
        return getRangeType() == RangeType.NORMAL;
    }

    public default boolean isPrefix() {
        return getRangeType() == RangeType.PREFIX;
    }

    public default void setColumn(String column) {
        if (getRangeType() == RangeType.NORMAL) {
            logger.error("ColumnsInterval can't not use the setColumn func");
        }
    }

    public default String getColumn() {
        logger.error("ColumnsInterval can't not use the getColumn func");
        return null;
    }

    public default String getStartColumn() {
        if (getRangeType() == RangeType.PREFIX) {
            logger.error("ColumnsPrefixRange can't not use the getStartColumn func");
            return null;
        }
        return null;
    }

    public default void setStartColumn(String startColumn) {
        if (getRangeType() == RangeType.PREFIX) {
            logger.error("ColumnsPrefixRange can't not use the setStartColumn func");
        }
    }

    public default String getEndColumn() {
        if (getRangeType() == RangeType.PREFIX) {
            logger.error("ColumnsPrefixRange can't not use the getEndColumn func");
            return null;
        }
        return null;
    }

    public default void setEndColumn(String endColumn) {
        if (getRangeType() == RangeType.PREFIX) {
            logger.error("ColumnsPrefixRange can't not use the setEndColumn func");
        }
    }

    public String getSchemaPrefix();

    public void setSchemaPrefix(String schemaPrefix);

    public default boolean isCompletelyAfter(ColumnsRange colRange) {
        return false;
    }

    public default boolean isAfter(String colName) {
        return false;
    }

    public boolean isClosed();

    public void setClosed(boolean closed);

    // Strange function: it should not work on the implementation of TimeSeriesPrefixRange
    public static ColumnsRange fromString(String str) throws IllegalArgumentException {
        if (str.contains("-") && !isContainSpecialChar(str)) {
            String[] parts = str.split("-");
            if (parts.length != 2) {
                logger.error("Input string {} in invalid format of ColumnsInterval ", str);
                throw new IllegalArgumentException("Input invalid string format in ColumnsRange");
            }
            return new ColumnsInterval(
                    parts[0].equals("null") ? null : parts[0],
                    parts[1].equals("null") ? null : parts[1]);
        } else {
            if (str.contains(".*") && str.indexOf(".*") == str.length() - 2)
                str = str.substring(0, str.length() - 2);
            if (!isContainSpecialChar(str)) return new ColumnsPrefixRange(str);
            else {
                logger.error("Input string {} in invalid format of ColumnsPrefixRange ", str);
                throw new IllegalArgumentException("Input invalid string format in ColumnsRange");
            }
        }
    }

    public boolean isContain(String colName);

    public boolean isIntersect(ColumnsRange colRange);

    public int compareTo(ColumnsRange o);
}
