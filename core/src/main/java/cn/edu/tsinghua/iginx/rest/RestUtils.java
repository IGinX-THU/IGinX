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
package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class RestUtils {

  public static final String CATEGORY = "" + '\u2E84'; // "category";
  public static final long TOP_KEY = 9223372036854775804L;
  public static final long DESCRIPTION_KEY = 9223372036854775805L;
  public static final long TITLE_KEY = 9223372036854775806L;
  public static final long MAX_KEY = 9223372036854775807L;
  public static final long ANNOTATION_START_KEY = 10L;
  public static final String ANNOTATION_SEQUENCE = "title.description";

  public static DataType checkType(SessionQueryDataSet sessionQueryDataSet) {
    int n = sessionQueryDataSet.getKeys().length;
    int m = sessionQueryDataSet.getPaths().size();
    int ret = 0;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < m; j++) {
        if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
          if (sessionQueryDataSet.getValues().get(i).get(j) instanceof Integer
              || sessionQueryDataSet.getValues().get(i).get(j) instanceof Long) {
            ret = Math.max(ret, 1);
          } else if (sessionQueryDataSet.getValues().get(i).get(j) instanceof Float
              || sessionQueryDataSet.getValues().get(i).get(j) instanceof Double) {
            ret = Math.max(ret, 2);
          } else if (sessionQueryDataSet.getValues().get(i).get(j) instanceof byte[]) {
            ret = 3;
          }
        }
      }
    }
    switch (ret) {
      case 0:
        return DataType.BOOLEAN;
      case 1:
        return DataType.LONG;
      case 2:
        return DataType.DOUBLE;
      case 3:
      default:
        return DataType.BINARY;
    }
  }

  public static long getInterval(long key, long startKey, long duration) {
    return (key - startKey) / duration;
  }

  public static long getIntervalStart(long key, long startKey, long duration) {
    return (key - startKey) / duration * duration + startKey;
  }

  public static DataType judgeObjectType(Object obj) {
    if (obj instanceof Boolean) {
      return DataType.BOOLEAN;
    } else if (obj instanceof Byte || obj instanceof String || obj instanceof Character) {
      return DataType.BINARY;
    } else if (obj instanceof Long || obj instanceof Integer) {
      return DataType.LONG;
    } else if (obj instanceof Double || obj instanceof Float) {
      return DataType.DOUBLE;
    }
    // 否则默认字符串类型
    return DataType.BINARY;
  }
}
