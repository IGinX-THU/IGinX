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
import static cn.edu.tsinghua.iginx.integration.controller.Controller.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLExecutor.class);

  private ExecutorService pool = Executors.newFixedThreadPool(30);

  private final MultiConnection conn;

  private boolean needCompareResult = true;

  public SQLExecutor(MultiConnection session) {
    this.conn = session;
  }

  public void open() throws SessionException {
    if (pool.isShutdown()) pool = Executors.newFixedThreadPool(30);
    conn.openSession();
  }

  public void close() throws SessionException {
    pool.shutdownNow();
    try {
      conn.closeSession();
    } finally {
      try {
        if (!pool.awaitTermination(5, TimeUnit.SECONDS))
          throw new AssertionError("Concurrent SQL workers did not terminate");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted while closing SQL workers", e);
      }
    }
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
    List<Callable<Void>> tasks = new ArrayList<>();
    for (String statement : statements)
      tasks.add(
          () -> {
            execute(statement);
            return null;
          });
    runConcurrent(tasks);
  }

  public void concurrentExecuteAndCompare(List<Pair<String, String>> statementsAndExpectRes) {
    List<Callable<Void>> tasks = new ArrayList<>();
    for (Pair<String, String> pair : statementsAndExpectRes)
      tasks.add(
          () -> {
            executeAndCompare(
                pair.getK(), pair.getV(), pair.getK().toLowerCase().startsWith("show columns"));
            return null;
          });
    runConcurrent(tasks);
  }

  private void runConcurrent(List<Callable<Void>> tasks) {
    CompletionService<Void> completed = new ExecutorCompletionService<>(pool);
    List<Future<Void>> futures = new ArrayList<>();
    // A single release gate also works when there are more tasks than workers.
    CountDownLatch start = new CountDownLatch(1);
    long deadline =
        System.nanoTime()
            + TimeUnit.SECONDS.toNanos(Long.getLong("iginx.test.concurrentTimeoutSeconds", 120L));
    try {
      for (Callable<Void> task : tasks)
        futures.add(
            completed.submit(
                () -> {
                  start.await();
                  return task.call();
                }));
      start.countDown();
      for (int i = 0; i < tasks.size(); i++) {
        Future<Void> result =
            completed.poll(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        if (result == null)
          throw new AssertionError("Concurrent SQL timed out; completed " + i + "/" + tasks.size());
        result.get();
      }
    } catch (ExecutionException e) {
      throw new AssertionError("Concurrent SQL failed", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Concurrent SQL interrupted", e);
    } finally {
      start.countDown();
      for (Future<Void> future : futures) if (!future.isDone()) future.cancel(true);
    }
  }
}
