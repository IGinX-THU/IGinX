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
package cn.edu.tsinghua.iginx.transform.exec.tools;

import static cn.edu.tsinghua.iginx.transform.utils.Constants.TEMP_TABLE_NAME;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Data;

public class ExecutionMetaManager {

  @Data
  static class ExecutionMeta {
    String tempTableName;

    public ExecutionMeta(String tempTableName) {
      this.tempTableName = tempTableName;
    }
  }

  private static final ThreadLocal<ExecutionMeta> metaThreadLocal = new ThreadLocal<>();

  public static void setMeta(String tempTableName) {
    metaThreadLocal.set(new ExecutionMeta(tempTableName));
  }

  public static String getTempTableName() {
    return metaThreadLocal.get().getTempTableName();
  }

  /** replace placeholders in sql to actual temp table name */
  public static String replaceTableNameIgnoreCase(String sql) {
    Pattern pattern = Pattern.compile(Pattern.quote(TEMP_TABLE_NAME), Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(sql);
    StringBuffer sb = new StringBuffer();

    while (matcher.find()) {
      matcher.appendReplacement(sb, Matcher.quoteReplacement(getTempTableName()));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }
}
