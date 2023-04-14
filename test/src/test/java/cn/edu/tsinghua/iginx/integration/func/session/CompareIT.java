package cn.edu.tsinghua.iginx.integration.func.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompareIT {

    private static final Logger logger = LoggerFactory.getLogger(CompareIT.class);

    protected static MultiConnection conn;
    protected static boolean isForSession = true;
    protected static boolean isForSessionPool = false;

    // host info
    protected static String defaultTestHost = "127.0.0.1";
    protected static int defaultTestPort = 6888;
    protected static String defaultTestUser = "root";
    protected static String defaultTestPass = "root";

    private static final long START_KEY = 0L;

    private static final long END_KEY = 15000L;

    private final List<String> testSQLGroupA = new ArrayList<>();

    private final List<String> testSQLGroupB = new ArrayList<>();

    public CompareIT() {
        readFile(testSQLGroupA,
            Paths.get("src", "test", "resources", "compare", "testGroupA.txt").toString());
        readFile(testSQLGroupB,
            Paths.get("src", "test", "resources", "compare", "testGroupB.txt").toString());
    }

    private void readFile(List<String> testSQLGroup, String filename) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line = reader.readLine();
            while (line != null) {
                testSQLGroup.add(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            logger.error("read file failed, filename: {}, cause: {}", filename, e.getMessage());
            fail();
        }
    }

    @BeforeClass
    public static void setUp() throws SessionException {
        if (isForSession) {
            conn =
                new MultiConnection(
                    new Session(
                        defaultTestHost,
                        defaultTestPort,
                        defaultTestUser,
                        defaultTestPass));
        } else if (isForSessionPool) {
            conn =
                new MultiConnection(
                    new SessionPool(
                        new ArrayList<IginxInfo>() {
                            {
                                add(
                                    new IginxInfo.Builder()
                                        .host("0.0.0.0")
                                        .port(6888)
                                        .user("root")
                                        .password("root")
                                        .build());

                                add(
                                    new IginxInfo.Builder()
                                        .host("0.0.0.0")
                                        .port(7888)
                                        .user("root")
                                        .password("root")
                                        .build());
                            }
                        }));
        }
        conn.openSession();
    }

    @AfterClass
    public static void tearDown() throws SessionException {
        conn.closeSession();
    }

    @Before
    public void insertData() throws ExecutionException, SessionException {
        String insertStrPrefix = "INSERT INTO us.d1 (key, s1, s2, s3, s4) values ";

        StringBuilder builder = new StringBuilder(insertStrPrefix);

        int size = (int) (END_KEY - START_KEY);
        for (int i = 0; i < size; i++) {
            builder.append(", ");
            builder.append("(");
            builder.append(START_KEY + i).append(", ");
            builder.append(i).append(", ");
            builder.append(i + 1).append(", ");
            builder.append("\"")
                .append(new String(RandomStringUtils.randomAlphanumeric(10).getBytes()))
                .append("\", ");
            builder.append((i + 0.1));
            builder.append(")");
        }
        builder.append(";");

        String insertStatement = builder.toString();

        SessionExecuteSqlResult res = conn.executeSql(insertStatement);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Insert date execute fail. Caused by: {}.", res.getParseErrorMsg());
            fail();
        }
    }

    @After
    public void clearData() throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = null;
        try {
            res = conn.executeSql(clearData);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}", clearData, e.toString());
            if (e.toString().equals(Controller.CLEARDATAEXCP)) {
                logger.error("clear data fail and go on....");
            } else {
                fail();
            }
        }

        if (res != null && res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error(
                "Statement: \"{}\" execute fail. Caused by: {}.",
                clearData,
                res.getParseErrorMsg());
            fail();
        }
    }

    private void executeAndCompare(String sqlA, String sqlB) {
        String resultA = execute(sqlA);
        String resultB = execute(sqlB);
        assertEquals(resultA, resultB);
    }

    private String execute(String statement) {
        if (!statement.toLowerCase().startsWith("insert")) {
            logger.info("Execute Statement: \"{}\"", statement);
        }

        SessionExecuteSqlResult res = null;
        try {
            res = conn.executeSql(statement);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
            fail();
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

    @Test
    public void compareTest() {
        if (testSQLGroupA.size() != testSQLGroupB.size()) {
            logger.error("two test groups' size are not equal.");
            fail();
        }

        for (int i = 0; i < testSQLGroupA.size(); i++) {
            String sqlA = testSQLGroupA.get(i);
            String sqlB = testSQLGroupB.get(i);
            executeAndCompare(sqlA, sqlB);
        }
    }
}
