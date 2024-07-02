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
package cn.edu.tsinghua.iginx.integration.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionIT {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CompactionIT.class);
  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";
  protected static MultiConnection session;

  @Before
  public void setUp() {
    session =
        new MultiConnection(
            new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
    try {
      session.openSession();
      session.executeSql("SET CONFIG \"enableInstantCompaction\" \"true\";");
      session.executeSql("SHOW CONFIG \"enableInstantCompaction\";").print(false, "");
    } catch (SessionException e) {
      LOGGER.error("unexpected error: ", e);
      fail();
    }
  }

  @After
  public void tearDown() {
    try {
      session.executeSql("SET CONFIG \"enableInstantCompaction\" \"false\";");
      session.closeSession();
    } catch (SessionException e) {
      LOGGER.error("unexpected error: ", e);
      fail();
    }
  }

  @Test
  public void testCompact() throws SessionException {
    String insertStrPrefix = "INSERT INTO us.d1 (key, s1, s2, s3, s4) values ";

    StringBuilder builder = new StringBuilder(insertStrPrefix);

    for (int i = 0; i < 10; i++) {
      if (i != 0) {
        builder.append(", ");
      }
      builder.append("(");
      builder.append(i).append(", ");
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
    session.executeSql(insertStatement);

    String selectSql1 = "SELECT s1 from us.d1;";
    String selectSql2 = "SELECT s4 from us.d1;";
    String selectSql1Output = session.executeSql(selectSql1).getResultInString(false, "");
    String sql1Output =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s1|\n"
            + "+---+--------+\n"
            + "|  0|       0|\n"
            + "|  1|       1|\n"
            + "|  2|       2|\n"
            + "|  3|       3|\n"
            + "|  4|       4|\n"
            + "|  5|       5|\n"
            + "|  6|       6|\n"
            + "|  7|       7|\n"
            + "|  8|       8|\n"
            + "|  9|       9|\n"
            + "+---+--------+\n"
            + "Total line number = 10\n";
    assertEquals(sql1Output, selectSql1Output);
    String selectSql2Output = session.executeSql(selectSql2).getResultInString(false, "");
    String sql2Output =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s4|\n"
            + "+---+--------+\n"
            + "|  0|     0.1|\n"
            + "|  1|     1.1|\n"
            + "|  2|     2.1|\n"
            + "|  3|     3.1|\n"
            + "|  4|     4.1|\n"
            + "|  5|     5.1|\n"
            + "|  6|     6.1|\n"
            + "|  7|     7.1|\n"
            + "|  8|     8.1|\n"
            + "|  9|     9.1|\n"
            + "+---+--------+\n"
            + "Total line number = 10\n";
    assertEquals(sql2Output, selectSql2Output);

    session.executeSql("COMPACT;");
    selectSql1Output = session.executeSql(selectSql1).getResultInString(false, "");
    assertEquals(sql1Output, selectSql1Output);
    selectSql2Output = session.executeSql(selectSql2).getResultInString(false, "");
    assertEquals(sql2Output, selectSql2Output);

    // 足够的时间等待清理完成
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      LOGGER.error("unexpected error: ", e);
    }

    selectSql1Output = session.executeSql(selectSql1).getResultInString(false, "");
    assertEquals(sql1Output, selectSql1Output);
    selectSql2Output = session.executeSql(selectSql2).getResultInString(false, "");
    assertEquals(sql2Output, selectSql2Output);
  }
}
