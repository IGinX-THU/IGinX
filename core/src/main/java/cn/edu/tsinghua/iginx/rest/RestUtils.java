/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class RestUtils {

  public static int errorStatus(Throwable error) {
    if (cn.edu.tsinghua.iginx.security.SecurityConfig.getInstance().isEnabled()) {
      for (Throwable cause = error; cause != null; cause = cause.getCause()) {
        if (cause instanceof javax.ws.rs.ForbiddenException || cause instanceof SecurityException)
          return 403;
        if (cause instanceof cn.edu.tsinghua.iginx.exception.SessionException
            && ((cn.edu.tsinghua.iginx.exception.SessionException) cause).getErrorCode()
                == cn.edu.tsinghua.iginx.utils.RpcUtils.ACCESS_DENY.getCode()) return 403;
      }
    }
    return 400;
  }

  public static final String CATEGORY = "" + '\u2E84'; // "category";
  public static final long TOP_KEY = 9223372036854775804L;
  public static final long DESCRIPTION_KEY = 9223372036854775805L;
  public static final long TITLE_KEY = 9223372036854775806L;
  public static final long MAX_KEY = 9223372036854775807L;
  public static final long ANNOTATION_START_KEY = 10L;
  public static final String ANNOTATION_SEQUENCE = "title.description";
  private static long annotationMillis = -1;
  private static int annotationSequence;

  /** Unique cluster annotation references representable exactly by both LONG and DOUBLE metrics. */
  public static synchronized long nextAnnotationId() throws InterruptedException {
    long now = System.currentTimeMillis();
    if (now < annotationMillis) throw new IllegalStateException("Clock moved backwards");
    if (now == annotationMillis && annotationSequence == 8) {
      do {
        Thread.sleep(1);
        now = System.currentTimeMillis();
      } while (now <= annotationMillis);
    }
    if (now != annotationMillis) {
      annotationMillis = now;
      annotationSequence = 0;
    }
    long elapsed = now - 1577836800000L; // 2020-01-01 UTC
    long node = cn.edu.tsinghua.iginx.metadata.DefaultMetaManager.getInstance().getIginxId();
    if (elapsed < 0 || elapsed >= (1L << 41) || node < 0 || node >= 512)
      throw new IllegalStateException("Annotation identifier space exhausted");
    return (elapsed << 12) | (node << 3) | annotationSequence++;
  }

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
