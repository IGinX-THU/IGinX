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
package cn.edu.tsinghua.iginx.integration.tool;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
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
    String actualOutput = execute(statement);
    if (!needCompareResult) {
      return;
    }
    assertEquals(expectedOutput, actualOutput);
  }

  public void executeAndCompareErrMsg(String statement, String expectedErrMsg) {
    LOGGER.info("Execute Statement: \"{}\"", statement);

    try {
      conn.executeSql(statement);
    } catch (SessionException e) {
      LOGGER.info("Statement: \"{}\" execute fail. Because: ", statement, e);
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
            start.countDown();

            try {
              start.await();
            } catch (InterruptedException e) {
              LOGGER.error("Interrupt when latch await");
            }

            String actualOutput = execute(statement);
            if (!expected.equals(actualOutput)) {
              failedList.add(new Pair<>(statement, new Pair<>(expected, actualOutput)));
            }
            end.countDown();
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
