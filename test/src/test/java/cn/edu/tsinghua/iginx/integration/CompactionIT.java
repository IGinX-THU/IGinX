package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class CompactionIT {
    protected static final Logger logger = LoggerFactory.getLogger(CompactionIT.class);
    //host info
    protected static String defaultTestHost = "127.0.0.1";
    protected static int defaultTestPort = 6888;
    protected static String defaultTestUser = "root";
    protected static String defaultTestPass = "root";
    protected static MultiConnection session;

    @Before
    public void setUp() {
        ConfigDescriptor.getInstance().getConfig().setEnableInstantCompaction(true);
        session = new MultiConnection(
                new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
        try {
            session.openSession();
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    @After
    public void tearDown() throws SessionException, ExecutionException {
        session.executeSql("CLEAR DATA");
        ConfigDescriptor.getInstance().getConfig().setEnableInstantCompaction(false);
        session.closeSession();
    }

    @Test
    public void testCompact() throws ExecutionException, SessionException {
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
            builder.append("\"")
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
        String sql1Output = "ResultSets:\n" +
                "+---+--------+\n" +
                "|key|us.d1.s1|\n" +
                "+---+--------+\n" +
                "|  0|       0|\n" +
                "|  1|       1|\n" +
                "|  2|       2|\n" +
                "|  3|       3|\n" +
                "|  4|       4|\n" +
                "|  5|       5|\n" +
                "|  6|       6|\n" +
                "|  7|       7|\n" +
                "|  8|       8|\n" +
                "|  9|       9|\n" +
                "+---+--------+\n" +
                "Total line number = 10\n";
        assertEquals(sql1Output, selectSql1Output);
        String selectSql2Output = session.executeSql(selectSql2).getResultInString(false, "");
        String sql2Output = "ResultSets:\n" +
                "+---+--------+\n" +
                "|key|us.d1.s4|\n" +
                "+---+--------+\n" +
                "|  0|     0.1|\n" +
                "|  1|     1.1|\n" +
                "|  2|     2.1|\n" +
                "|  3|     3.1|\n" +
                "|  4|     4.1|\n" +
                "|  5|     5.1|\n" +
                "|  6|     6.1|\n" +
                "|  7|     7.1|\n" +
                "|  8|     8.1|\n" +
                "|  9|     9.1|\n" +
                "+---+--------+\n" +
                "Total line number = 10\n";
        assertEquals(sql2Output, selectSql2Output);

        session.executeSql("COMPACT");
        selectSql1Output = session.executeSql(selectSql1).getResultInString(false, "");
        assertEquals(sql1Output, selectSql1Output);
        selectSql2Output = session.executeSql(selectSql2).getResultInString(false, "");
        assertEquals(sql2Output, selectSql2Output);

        // 足够的时间等待清理完成
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        selectSql1Output = session.executeSql(selectSql1).getResultInString(false, "");
        assertEquals(sql1Output, selectSql1Output);
        selectSql2Output = session.executeSql(selectSql2).getResultInString(false, "");
        assertEquals(sql2Output, selectSql2Output);
    }
}
