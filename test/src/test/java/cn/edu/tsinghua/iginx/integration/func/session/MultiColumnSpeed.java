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
package cn.edu.tsinghua.iginx.integration.func.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiColumnSpeed extends BaseSessionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiColumnSpeed.class);

  private String buildInsertStatement(int columnSize, int rowSize, int startKey) {
    StringBuilder builder = new StringBuilder("insert into wide_column(key,");
    for (int i = 0; i < columnSize; i++) {
      builder.append("c").append(i).append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(") values ");
    for (int i = 0; i < rowSize; i++) {
      builder.append("(").append(startKey + i).append(",");
      for (int j = 0; j < columnSize; j++) {
        Random random = new Random();
        // 生成[0, 100)的随机数
        builder.append(random.nextInt(100)).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append("),");
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(";");
    return builder.toString();
  }

  private String buildCreateTableStatement(int columnSize) {
    StringBuilder builder = new StringBuilder("insert into wide_column(key,");
    for (int i = 0; i < columnSize; i++) {
      builder.append("c").append(i).append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(") values ");
    builder.append("(").append(0).append(",");
    for (int j = 0; j < columnSize; j++) {
      Random random = new Random();
      // 生成[0, 100)的随机数
      builder.append(random.nextInt(100)).append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append("),");
    builder.deleteCharAt(builder.length() - 1);
    builder.append(";");
    return builder.toString();
  }

  @Test
  public void testMultiColumnSpeed() throws SessionException {
    int columnSize = 1000, rowSize = 1000;
    String create = buildCreateTableStatement(columnSize);
    long startTime, endTime;
    List<Long> insertCost = new ArrayList<>();
    clearData();
    // 先创建table，因为原有创建除第一个字段之外都是通过ALTER TABLE，无法很好衡量后续插入
    session.executeSql(create);

    for (int i = 0; i < 100; ++i) {
      startTime = System.currentTimeMillis();
      String insert = buildInsertStatement(columnSize, rowSize, i * 1000 + 1);
      session.executeSql(insert);
      endTime = System.currentTimeMillis();
      insertCost.add(endTime - startTime);
    }
    long insertTotalCost = insertCost.stream().mapToLong(Long::longValue).sum();
    LOGGER.info("insert {} columns and {} rows cost {} ms", columnSize, rowSize, insertTotalCost);
    clearData();
    String insert = buildInsertStatement(columnSize, rowSize, 0);
    session.executeSql(insert);
    String statement = "SELECT * FROM wide_column;";
    startTime = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      session.executeSql(statement);
    }
    endTime = System.currentTimeMillis();
    LOGGER.info("select rows cost {} ms", endTime - startTime);
  }
}
