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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import org.apache.commons.lang3.RandomStringUtils;

public class ParquetSessionExample {

  private static Session session;

  public static void main(String[] args) throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    // 打开 Session
    session.openSession();

    // 查看数据分区情况
    long startKey = 0L;
    long step = 10000L;
    for (int i = 0; i < 100; i++) {
      System.out.println("start insert batch data: " + i);
      insertData(startKey, startKey + step);
      startKey += step;
    }

    // 关闭 Session
    session.closeSession();
  }

  private static void insertData(long startKey, long endKey) throws SessionException {
    String insertStrPrefix = "INSERT INTO us.d1 (key, s1, s2, s3, s4) values ";

    StringBuilder builder = new StringBuilder(insertStrPrefix);

    int size = (int) (endKey - startKey);
    for (int i = 0; i < size; i++) {
      builder.append(", ");
      builder.append("(");
      builder.append(startKey + i).append(", ");
      builder.append(i).append(", ");
      builder.append(i + 1).append(", ");
      builder
          .append("\"")
          .append(new String(RandomStringUtils.randomAlphanumeric(10).getBytes()))
          .append("\", ");
      builder.append((i + 0.1));
      builder.append(")");
    }
    builder.append(";");

    String insertStatement = builder.toString();

    SessionExecuteSqlResult res = session.executeSql(insertStatement);
    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      System.out.printf("Insert date execute fail. Caused by: %s.\n", res.getParseErrorMsg());
    }
  }
}
