package cn.edu.tsinghua.iginx.integration.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.Pair;
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
            logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
            if (e.toString().equals(Controller.CLEARDATAEXCP)) {
                logger.error("clear data fail and go on....");
                return "";
            } else {
                fail();
            }
        }

        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error(
                    "Statement: \"{}\" execute fail. Caused by: {}.",
                    statement,
                    res.getParseErrorMsg());
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
        CountDownLatch latch = new CountDownLatch(statementsAndExpectRes.size());

        for (Pair<String, String> pair : statementsAndExpectRes) {
            pool.submit(
                    () -> {
                        executeAndCompare(pair.getK(), pair.getV());
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
}
