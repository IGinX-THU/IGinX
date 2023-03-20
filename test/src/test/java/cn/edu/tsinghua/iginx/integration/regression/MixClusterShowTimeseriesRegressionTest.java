package cn.edu.tsinghua.iginx.integration.regression;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MixClusterShowTimeseriesRegressionTest {

    private static final Logger logger = LoggerFactory.getLogger(MixClusterShowTimeseriesRegressionTest.class);

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
    public void testShowTimeseriesInMixCluster() throws SessionException, ExecutionException {
        String[] insertStatements = new String[] {
                "insert into m (key, d, o) values (2000000, 2, 3)",
                "insert into m (key, a, z) values (3000000, 1, 2)",
                "insert into m (key, h, n) values (4000000, 2, 3)",
                "insert into m (key, d, o) values (1000000, 1, 2)",
                "insert into m (key, d, n) values (8000000, 9, 9)",
                "insert into m (key, p) values (8000000, 9)"
        };
        for (String insertStatement: insertStatements) {
            execute(insertStatement);
        }
        String statement = "show time series";
        System.out.println(execute(statement));

        statement = "show time series m.*";
        System.out.println(execute(statement));

        statement = "show time series m.a, m.d, m.p";
        System.out.println(statement);
    }

    private void executeAndCompare(String statement, String expectedOutput) {
        String actualOutput = execute(statement);
        assertEquals(expectedOutput, actualOutput);
    }

    private String execute(String statement) {
        if (!statement.toLowerCase().startsWith("insert")) {
            logger.info("Execute Statement: \"{}\"", statement);
        }

        SessionExecuteSqlResult res = null;
        try {
            res = session.executeSql(statement);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
            fail();
        }

        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}.", statement,
                    res.getParseErrorMsg());
            fail();
            return "";
        }

        return res.getResultInString(false, "");
    }



}
