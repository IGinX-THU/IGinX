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
package cn.edu.tsinghua.iginx.integration.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MixClusterShowColumnsRegressionTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MixClusterShowColumnsRegressionTest.class);

  private Session session;

  @Before
  public void setUp() throws SessionException {
    session = new Session("127.0.0.1", 6888);
    session.openSession();
  }

  @After
  public void tearDown() throws SessionException {
    session.closeSession();
    session = null;
  }

  @Test
  public void testShowColumnsInMixCluster() {
    String[] insertStatements =
        new String[] {
          "insert into m (key, d, o) values (2000000, 2, 3);",
          "insert into m (key, a, z) values (3000000, 1, 2);",
          "insert into m (key, h, n) values (4000000, 2, 3);",
          "insert into m (key, d, o) values (1000000, 1, 2);",
          "insert into m (key, d, n) values (8000000, 9, 9);",
          "insert into m (key, p) values (8000000, 9);",
          "insert into n (key, d, n) values (10000000, 11.0, 11.0);"
        };
    for (String insertStatement : insertStatements) {
      execute(insertStatement);
    }
    String statement = "SHOW COLUMNS;";
    String expected =
        "Columns:\n"
            + "+----+--------+\n"
            + "|Path|DataType|\n"
            + "+----+--------+\n"
            + "| m.a|    LONG|\n"
            + "| m.d|    LONG|\n"
            + "| m.h|    LONG|\n"
            + "| m.n|    LONG|\n"
            + "| m.o|    LONG|\n"
            + "| m.p|    LONG|\n"
            + "| m.z|    LONG|\n"
            + "| n.d|  DOUBLE|\n"
            + "| n.n|  DOUBLE|\n"
            + "+----+--------+\n"
            + "Total line number = 9\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS m.*;";
    expected =
        "Columns:\n"
            + "+----+--------+\n"
            + "|Path|DataType|\n"
            + "+----+--------+\n"
            + "| m.a|    LONG|\n"
            + "| m.d|    LONG|\n"
            + "| m.h|    LONG|\n"
            + "| m.n|    LONG|\n"
            + "| m.o|    LONG|\n"
            + "| m.p|    LONG|\n"
            + "| m.z|    LONG|\n"
            + "+----+--------+\n"
            + "Total line number = 7\n";
    executeAndCompare(statement, expected);
  }

  private void executeAndCompare(String statement, String expectedOutput) {
    String actualOutput = execute(statement);
    System.out.println(actualOutput);
    assertEquals(expectedOutput, actualOutput);
  }

  private String execute(String statement) {
    if (!statement.toLowerCase().startsWith("insert")) {
      LOGGER.info("Execute Statement: \"{}\"", statement);
    }

    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(statement);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.error(
          "Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
      fail();
      return "";
    }

    return res.getResultInString(false, "");
  }
}
