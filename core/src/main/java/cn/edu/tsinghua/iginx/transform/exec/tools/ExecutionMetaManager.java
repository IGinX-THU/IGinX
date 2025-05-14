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

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.TRANSFORM_PREFIX;
import static cn.edu.tsinghua.iginx.transform.utils.Constants.TEMP_TABLE_NAME_FORMAT;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;

/** 管理 python task 输出的临时表信息，管理用户定义的别名和真实的临时表名之间的映射关系 */
public class ExecutionMetaManager {

  private static final ThreadLocal<Map<String, String>> metaThreadLocal =
      ThreadLocal.withInitial(HashMap::new);

  public static void setTempTable(String aliasName, long jobId) {
    String tempTableName =
        String.format(TEMP_TABLE_NAME_FORMAT, jobId, RandomStringUtils.randomAlphanumeric(6));
    metaThreadLocal.get().put(aliasName, tempTableName);
  }

  public static String getTempTableName(String aliasName) {
    if (TRANSFORM_PREFIX.equals(aliasName)) {
      return TRANSFORM_PREFIX;
    }
    return metaThreadLocal.get().get(aliasName);
  }

  public static Collection<String> getTempTableNames() {
    return metaThreadLocal.get().values();
  }

  /** replace placeholders in sql to actual temp table name */
  public static String replaceTableNameIgnoreCase(String sql) {
    if (getTempTableNames().isEmpty()) {
      return sql;
    }
    String patternString =
        metaThreadLocal.get().keySet().stream()
            .map(Pattern::quote)
            .map(s -> "\\b" + s + "\\b") // whole word
            .collect(Collectors.joining("|"));

    Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(sql);
    StringBuffer result = new StringBuffer();

    while (matcher.find()) {
      String matched = matcher.group();
      String replacement = getTempTableName(matched);
      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);

    return result.toString();
  }
}
