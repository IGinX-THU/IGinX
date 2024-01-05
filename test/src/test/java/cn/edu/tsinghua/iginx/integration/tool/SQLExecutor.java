package cn.edu.tsinghua.iginx.integration.tool;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
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

  private static final Logger logger = LoggerFactory.getLogger(SQLExecutor.class);

  private final ExecutorService pool = Executors.newFixedThreadPool(30);

  private final MultiConnection conn;

  public SQLExecutor(MultiConnection session) {
    this.conn = session;
  }

  public void open() throws SessionException {
    conn.openSession();
  }

  public void close() throws SessionException {
    conn.closeSession();
  }

  public String execute(String statement) {
    if (statement.toLowerCase().startsWith("insert")) {
      logger.info("Execute Insert Statement.");
    } else {
      logger.info("Execute Statement: \"{}\"", statement);
    }

    SessionExecuteSqlResult res = null;
    try {
      res = conn.executeSql(statement);
    } catch (SessionException | ExecutionException e) {
      if (e.toString().trim().contains(CLEAR_DUMMY_DATA_CAUTION)) {
        logger.warn(CLEAR_DATA_WARNING);
        return "";
      } else {
        logger.error(CLEAR_DATA_ERROR, statement, e.getMessage());
        fail();
      }
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      logger.error(CLEAR_DATA_ERROR, statement, res.getParseErrorMsg());
      fail();
      return "";
    }

    return res.getResultInString(false, "");
  }

  public void executeAndCompare(String statement, String expectedOutput) {
    String actualOutput = execute(statement);
    assertEquals(expectedOutput, actualOutput);
  }

  public void executeAndCompareErrMsg(String statement, String expectedErrMsg) {
    logger.info("Execute Statement: \"{}\"", statement);

    try {
      conn.executeSql(statement);
    } catch (SessionException | ExecutionException e) {
      logger.info("Statement: \"{}\" execute fail. Because: {}", statement, e.getMessage());
      assertEquals(expectedErrMsg, e.getMessage());
    }
  }

  public void concurrentExecute(List<String> statements) {
    logger.info("Concurrent execute statements, size={}", statements.size());
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
      logger.error("Interrupt when latch await");
      fail();
    }
  }

  public void concurrentExecuteAndCompare(List<Pair<String, String>> statementsAndExpectRes) {
    logger.info("Concurrent execute statements, size={}", statementsAndExpectRes.size());
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
              logger.error("Interrupt when latch await");
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
      logger.error("Interrupt when latch await");
      fail();
    }

    if (!failedList.isEmpty()) {
      failedList.forEach(
          failed -> {
            logger.error(
                "Statement: \"{}\" execute result is inconsistent with the expectation.",
                failed.getK());
            logger.error("Expected: {}", failed.getV().getK());
            logger.error("Actual: {}", failed.getV().getV());
          });
      fail();
    }
  }
}
