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
package cn.edu.tsinghua.iginx.integration.tool;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.CLEAR_DATA_WARNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLExecutor.class);

  private final ExecutorService pool = Executors.newFixedThreadPool(30);

  private final MultiConnection conn;

  private boolean needCompareResult = true;

  public SQLExecutor(MultiConnection session) {
    this.conn = session;
  }

  public void open() throws SessionException {
    conn.openSession();
  }

  public void close() throws SessionException {
    conn.closeSession();
  }

  public void setNeedCompareResult(boolean needCompareResult) {
    this.needCompareResult = needCompareResult;
  }

  public SessionExecuteSqlResult getSessionExecuteSqlResult(String statement) {
    if (statement.toLowerCase().startsWith("insert")) {
      LOGGER.info("Execute Insert Statement.");
    } else {
      LOGGER.info("Execute Statement: \"{}\"", statement);
    }

    SessionExecuteSqlResult res = null;
    try {
      res = conn.executeSql(statement);
    } catch (SessionException e) {
      if (e.toString().trim().contains(CLEAR_DUMMY_DATA_CAUTION)) {
        LOGGER.warn(CLEAR_DATA_WARNING);
        return null;
      } else {
        LOGGER.error("Statement: \"{}\" execute fail. Caused by: ", statement, e);
        fail();
      }
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.error(
          "Statement: \"{}\" execute fail. Caused by: {}", statement, res.getParseErrorMsg());
      fail();
      return null;
    }

    return res;
  }

  public String execute(String statement) {
    SessionExecuteSqlResult res = getSessionExecuteSqlResult(statement);
    if (res == null) {
      return "";
    }
    return res.getResultInString(false, "");
  }

  public void executeAndCompare(String statement, String expectedOutput) {
    executeAndCompare(statement, expectedOutput, false);
  }

  public void executeAndCompare(String statement, String expectedOutput, boolean ignoreOrder) {
    String actualOutput = execute(statement);
    if (!needCompareResult) {
      return;
    }
    if (ignoreOrder) {
      if (!TestUtils.isResultSetEqual(expectedOutput, actualOutput)) {
        LOGGER.error(
            "Statement: \"{}\" execute fail,\nexpected:\"{}\",\nactual:\"{}\"",
            statement,
            expectedOutput,
            actualOutput);
        fail();
      }
    } else {
      assertEquals(expectedOutput, actualOutput);
    }
  }

  public void executeAndCompareLineCount(String statement, int expectedLineCount) {
    String actualOutput = execute(statement);
    if (!needCompareResult) {
      return;
    }
    List<String> actualLines = Arrays.asList(actualOutput.split("\n"));
    // 计算行数
    if (actualLines.get(actualLines.size() - 1).startsWith("Total line number = ")) {
      String lineCountStr =
          actualLines.get(actualLines.size() - 1).replace("Total line number = ", "");
      int actualLineCount = Integer.parseInt(lineCountStr.trim());
      // 比较行数
      assertEquals(expectedLineCount, actualLineCount);
    } else if (actualLines.get(actualLines.size() - 1).startsWith("Empty set.")) {
      assertEquals(expectedLineCount, 0);
    } else {
      fail();
    }
  }

  public void executeAndCompareErrMsg(String statement, String expectedErrMsg) {
    LOGGER.info("Execute Statement: \"{}\"", statement);

    try {
      conn.executeSql(statement);
    } catch (SessionException e) {
      LOGGER.info("Statement: \"{}\" execute fail, with message: {}.", statement, e.getMessage());
      assertEquals(expectedErrMsg, e.getMessage());
    }
  }

  public void concurrentExecute(List<String> statements) {
    LOGGER.info("Concurrent execute statements, size={}", statements.size());
    CountDownLatch latch = new CountDownLatch(statements.size());

    for (String statement : statements) {
      pool.submit(
          () -> {
            execute(statement);
            latch.countDown();
          });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      LOGGER.error("Interrupt when latch await");
      fail();
    }
  }

  public void concurrentExecuteAndCompare(List<Pair<String, String>> statementsAndExpectRes) {
    LOGGER.info("Concurrent execute statements, size={}", statementsAndExpectRes.size());
    List<Pair<String, Pair<String, String>>> failedList =
        Collections.synchronizedList(new ArrayList<>());
    CountDownLatch start = new CountDownLatch(statementsAndExpectRes.size());
    CountDownLatch end = new CountDownLatch(statementsAndExpectRes.size());

    for (Pair<String, String> pair : statementsAndExpectRes) {
      pool.submit(
          () -> {
            String statement = pair.getK();
            String expected = pair.getV();
            boolean ignoreOrder = statement.toLowerCase().startsWith("show columns");
            start.countDown();
            try {
              start.await();
              String actualOutput = execute(statement);
              if (ignoreOrder && !TestUtils.isResultSetEqual(expected, actualOutput)) {
                failedList.add(new Pair<>(statement, new Pair<>(expected, actualOutput)));
              } else if (!ignoreOrder && !expected.equals(actualOutput)) {
                failedList.add(new Pair<>(statement, new Pair<>(expected, actualOutput)));
              }
              LOGGER.info("Successfully execute statement: \"{}\"", statement);
            } catch (Throwable e) {
              LOGGER.error("Statement: \"{}\" execute fail. Caused by: ", statement, e);
              failedList.add(new Pair<>(statement, new Pair<>(expected, e.toString())));
            } finally {
              end.countDown();
            }
          });
    }

    try {
      end.await();
    } catch (InterruptedException e) {
      LOGGER.error("Interrupt when latch await");
      fail();
    }

    if (!needCompareResult) {
      return;
    }
    if (!failedList.isEmpty()) {
      failedList.forEach(
          failed -> {
            LOGGER.error(
                "Statement: \"{}\" execute result is inconsistent with the expectation.",
                failed.getK());
            LOGGER.error("Expected: {}", failed.getV().getK());
            LOGGER.error("Actual: {}", failed.getV().getV());
          });
      fail();
    }
  }
}
