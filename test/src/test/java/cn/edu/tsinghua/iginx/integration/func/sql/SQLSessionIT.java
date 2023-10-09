package cn.edu.tsinghua.iginx.integration.func.sql;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.DBConf.DBConfType;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.integration.tool.SQLExecutor;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLSessionIT {

  protected static SQLExecutor executor;

  protected static boolean isForSession = true, isForSessionPool = false;
  protected static int MaxMultiThreadTaskNum = -1;

  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  protected static final Logger logger = LoggerFactory.getLogger(SQLSessionIT.class);

  protected boolean isAbleToDelete;

  protected boolean isSupportChinesePath;

  protected boolean isSupportNumericalPath;

  protected boolean isSupportSpecialCharacterPath;

  protected boolean isAbleToShowColumns;

  protected boolean isScaling = false;

  private final long startKey = 0L;

  private final long endKey = 15000L;

  protected boolean isAbleToClearData = true;
  private static final int CONCURRENT_NUM = 5;

  public SQLSessionIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    this.isScaling = conf.isScaling();
    this.isAbleToClearData = dbConf.getEnumValue(DBConf.DBConfType.isAbleToClearData);
    this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    this.isAbleToShowColumns = dbConf.getEnumValue(DBConf.DBConfType.isAbleToShowColumns);
    this.isSupportChinesePath = dbConf.getEnumValue(DBConfType.isSupportChinesePath);
    this.isSupportNumericalPath = dbConf.getEnumValue(DBConfType.isSupportNumericalPath);
    this.isSupportSpecialCharacterPath =
        dbConf.getEnumValue(DBConfType.isSupportSpecialCharacterPath);
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    MultiConnection session;
    if (isForSession) {
      session =
          new MultiConnection(
              new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
    } else if (isForSessionPool) {
      session =
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
    } else {
      logger.error("isForSession=false, isForSessionPool=false");
      fail();
      return;
    }
    executor = new SQLExecutor(session);
    executor.open();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    executor.close();
  }

  @Before
  public void insertData() {
    String insertStatement = generateDefaultInsertStatementByTimeRange(startKey, endKey);
    executor.execute(insertStatement);
  }

  private String generateDefaultInsertStatementByTimeRange(long start, long end) {
    String insertStrPrefix = "INSERT INTO us.d1 (key, s1, s2, s3, s4) values ";

    StringBuilder builder = new StringBuilder(insertStrPrefix);

    int size = (int) (end - start);
    for (int i = 0; i < size; i++) {
      builder.append(", ");
      builder.append("(");
      builder.append(start + i).append(", ");
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

    return builder.toString();
  }

  @After
  public void clearData() {
    String clearData = "CLEAR DATA;";
    executor.execute(clearData);
  }

  @Test
  public void testCountPath() {
    String statement = "SELECT COUNT(*) FROM us.d1;";
    String expected =
        "ResultSets:\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|          15000|          15000|          15000|          15000|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testCountPoints() {
    if (isScaling) return;
    String statement = "COUNT POINTS;";
    String expected = "Points num: 60000\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testShowTimeSeries() {
    if (!isAbleToShowColumns || isScaling) {
      return;
    }
    String statement = "SHOW COLUMNS us.*;";
    String expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s1|    LONG|\n"
            + "|us.d1.s2|    LONG|\n"
            + "|us.d1.s3|  BINARY|\n"
            + "|us.d1.s4|  DOUBLE|\n"
            + "+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS us.d1.*;";
    expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s1|    LONG|\n"
            + "|us.d1.s2|    LONG|\n"
            + "|us.d1.s3|  BINARY|\n"
            + "|us.d1.s4|  DOUBLE|\n"
            + "+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS limit 3;";
    expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s1|    LONG|\n"
            + "|us.d1.s2|    LONG|\n"
            + "|us.d1.s3|  BINARY|\n"
            + "+--------+--------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS limit 2 offset 1;";
    expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s2|    LONG|\n"
            + "|us.d1.s3|  BINARY|\n"
            + "+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS limit 1, 2;";
    expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s2|    LONG|\n"
            + "|us.d1.s3|  BINARY|\n"
            + "+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS us.d1.s1;";
    expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s1|    LONG|\n"
            + "+--------+--------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS us.d1.s1, us.d1.s3;";
    expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s1|    LONG|\n"
            + "|us.d1.s3|  BINARY|\n"
            + "+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testShowReplicaNum() {
    String statement = "SHOW REPLICA NUMBER;";
    String expected = "Replica num: 1\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testTimeRangeQuery() {
    String statement = "SELECT s1 FROM us.d1 WHERE key > 100 AND key < 120;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s1|\n"
            + "+---+--------+\n"
            + "|101|     101|\n"
            + "|102|     102|\n"
            + "|103|     103|\n"
            + "|104|     104|\n"
            + "|105|     105|\n"
            + "|106|     106|\n"
            + "|107|     107|\n"
            + "|108|     108|\n"
            + "|109|     109|\n"
            + "|110|     110|\n"
            + "|111|     111|\n"
            + "|112|     112|\n"
            + "|113|     113|\n"
            + "|114|     114|\n"
            + "|115|     115|\n"
            + "|116|     116|\n"
            + "|117|     117|\n"
            + "|118|     118|\n"
            + "|119|     119|\n"
            + "+---+--------+\n"
            + "Total line number = 19\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testValueFilter() {
    String query = "SELECT s1 FROM us.d1 WHERE key > 0 AND key < 10000 and s1 > 200 and s1 < 210;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s1|\n"
            + "+---+--------+\n"
            + "|201|     201|\n"
            + "|202|     202|\n"
            + "|203|     203|\n"
            + "|204|     204|\n"
            + "|205|     205|\n"
            + "|206|     206|\n"
            + "|207|     207|\n"
            + "|208|     208|\n"
            + "|209|     209|\n"
            + "+---+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(query, expected);

    String insert =
        "INSERT INTO us.d2(key, c) VALUES (1, \"asdas\"), (2, \"sadaa\"), (3, \"sadada\"), (4, \"asdad\"), (5, \"deadsa\"), (6, \"dasda\"), (7, \"asdsad\"), (8, \"frgsa\"), (9, \"asdad\");";
    executor.execute(insert);

    query = "SELECT c FROM us.d2 WHERE c like \"^a.*\";";
    expected =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|us.d2.c|\n"
            + "+---+-------+\n"
            + "|  1|  asdas|\n"
            + "|  4|  asdad|\n"
            + "|  7| asdsad|\n"
            + "|  9|  asdad|\n"
            + "+---+-------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT c FROM us.d2 WHERE c like \"^[s|f].*\"";
    expected =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|us.d2.c|\n"
            + "+---+-------+\n"
            + "|  2|  sadaa|\n"
            + "|  3| sadada|\n"
            + "|  8|  frgsa|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT c FROM us.d2 WHERE c like \"^.*[s|d]\";";
    expected =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|us.d2.c|\n"
            + "+---+-------+\n"
            + "|  1|  asdas|\n"
            + "|  4|  asdad|\n"
            + "|  7| asdsad|\n"
            + "|  9|  asdad|\n"
            + "+---+-------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    StringBuilder builder = new StringBuilder();
    builder.append("INSERT INTO us.d2(key, s1) VALUES ");
    int size = (int) (endKey - startKey);
    for (int i = 0; i < size; i++) {
      builder.append(", (");
      builder.append(startKey + i).append(", ");
      builder.append(i + 5);
      builder.append(")");
    }
    builder.append(";");

    insert = builder.toString();
    executor.execute(insert);

    query = "SELECT s1 FROM us.* WHERE s1 > 200 and s1 < 210;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|us.d1.s1|us.d2.s1|\n"
            + "+---+--------+--------+\n"
            + "|201|     201|     206|\n"
            + "|202|     202|     207|\n"
            + "|203|     203|     208|\n"
            + "|204|     204|     209|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testPathFilter() {
    String insert =
        "INSERT INTO us.d9(key, a, b) VALUES (1, 1, 9), (2, 2, 8), (3, 3, 7), (4, 4, 6), (5, 5, 5), (6, 6, 4), (7, 7, 3), (8, 8, 2), (9, 9, 1);";
    executor.execute(insert);

    String query = "SELECT a, b FROM us.d9 WHERE a > b;";
    String expected =
        "ResultSets:\n"
            + "+---+-------+-------+\n"
            + "|key|us.d9.a|us.d9.b|\n"
            + "+---+-------+-------+\n"
            + "|  6|      6|      4|\n"
            + "|  7|      7|      3|\n"
            + "|  8|      8|      2|\n"
            + "|  9|      9|      1|\n"
            + "+---+-------+-------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT a, b FROM us.d9 WHERE a >= b;";
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+\n"
            + "|key|us.d9.a|us.d9.b|\n"
            + "+---+-------+-------+\n"
            + "|  5|      5|      5|\n"
            + "|  6|      6|      4|\n"
            + "|  7|      7|      3|\n"
            + "|  8|      8|      2|\n"
            + "|  9|      9|      1|\n"
            + "+---+-------+-------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT a, b FROM us.d9 WHERE a < b;";
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+\n"
            + "|key|us.d9.a|us.d9.b|\n"
            + "+---+-------+-------+\n"
            + "|  1|      1|      9|\n"
            + "|  2|      2|      8|\n"
            + "|  3|      3|      7|\n"
            + "|  4|      4|      6|\n"
            + "+---+-------+-------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT a, b FROM us.d9 WHERE a <= b;";
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+\n"
            + "|key|us.d9.a|us.d9.b|\n"
            + "+---+-------+-------+\n"
            + "|  1|      1|      9|\n"
            + "|  2|      2|      8|\n"
            + "|  3|      3|      7|\n"
            + "|  4|      4|      6|\n"
            + "|  5|      5|      5|\n"
            + "+---+-------+-------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT a, b FROM us.d9 WHERE a = b;";
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+\n"
            + "|key|us.d9.a|us.d9.b|\n"
            + "+---+-------+-------+\n"
            + "|  5|      5|      5|\n"
            + "+---+-------+-------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT a, b FROM us.d9 WHERE a != b;";
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+\n"
            + "|key|us.d9.a|us.d9.b|\n"
            + "+---+-------+-------+\n"
            + "|  1|      1|      9|\n"
            + "|  2|      2|      8|\n"
            + "|  3|      3|      7|\n"
            + "|  4|      4|      6|\n"
            + "|  6|      6|      4|\n"
            + "|  7|      7|      3|\n"
            + "|  8|      8|      2|\n"
            + "|  9|      9|      1|\n"
            + "+---+-------+-------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testDistinct() {
    String insert =
        "INSERT INTO test(key, a, b) values (1, 1, 1), (2, 2, 1), (3, 2, 2), (4, 3, 1), (5, 3, 2), (6, 3, 1), (7, 4, 1), (8, 4, 2), (9, 4, 3), (10, 4, 1);";
    executor.execute(insert);

    String statement = "SELECT * FROM test;";
    String expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  1|     1|     1|\n"
            + "|  2|     2|     1|\n"
            + "|  3|     2|     2|\n"
            + "|  4|     3|     1|\n"
            + "|  5|     3|     2|\n"
            + "|  6|     3|     1|\n"
            + "|  7|     4|     1|\n"
            + "|  8|     4|     2|\n"
            + "|  9|     4|     3|\n"
            + "| 10|     4|     1|\n"
            + "+---+------+------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT DISTINCT a FROM test;";
    expected =
        "ResultSets:\n"
            + "+------+\n"
            + "|test.a|\n"
            + "+------+\n"
            + "|     1|\n"
            + "|     2|\n"
            + "|     3|\n"
            + "|     4|\n"
            + "+------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT DISTINCT a, b FROM test;";
    expected =
        "ResultSets:\n"
            + "+------+------+\n"
            + "|test.a|test.b|\n"
            + "+------+------+\n"
            + "|     1|     1|\n"
            + "|     2|     1|\n"
            + "|     2|     2|\n"
            + "|     3|     1|\n"
            + "|     3|     2|\n"
            + "|     4|     1|\n"
            + "|     4|     2|\n"
            + "|     4|     3|\n"
            + "+------+------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT COUNT(a), AVG(a), SUM(a), MIN(a), MAX(a) FROM test;";
    expected =
        "ResultSets:\n"
            + "+-------------+-----------+-----------+-----------+-----------+\n"
            + "|count(test.a)|avg(test.a)|sum(test.a)|min(test.a)|max(test.a)|\n"
            + "+-------------+-----------+-----------+-----------+-----------+\n"
            + "|           10|        3.0|         30|          1|          4|\n"
            + "+-------------+-----------+-----------+-----------+-----------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT COUNT(DISTINCT a), AVG(DISTINCT a), SUM(DISTINCT a), MIN(DISTINCT a), MAX(DISTINCT a) FROM test;";
    expected =
        "ResultSets:\n"
            + "+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "|count(distinct test.a)|avg(distinct test.a)|sum(distinct test.a)|min(distinct test.a)|max(distinct test.a)|\n"
            + "+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "|                     4|                 2.5|                  10|                   1|                   4|\n"
            + "+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, COUNT(b), AVG(b), SUM(b), MIN(b), MAX(b) FROM test GROUP BY a;";
    expected =
        "ResultSets:\n"
            + "+------+-------------+------------------+-----------+-----------+-----------+\n"
            + "|test.a|count(test.b)|       avg(test.b)|sum(test.b)|min(test.b)|max(test.b)|\n"
            + "+------+-------------+------------------+-----------+-----------+-----------+\n"
            + "|     1|            1|               1.0|          1|          1|          1|\n"
            + "|     2|            2|               1.5|          3|          1|          2|\n"
            + "|     3|            3|1.3333333333333333|          4|          1|          2|\n"
            + "|     4|            4|              1.75|          7|          1|          3|\n"
            + "+------+-------------+------------------+-----------+-----------+-----------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT a, COUNT(DISTINCT b), AVG(DISTINCT b), SUM(DISTINCT b), MIN(DISTINCT b), MAX(DISTINCT b) FROM test GROUP BY a;";
    expected =
        "ResultSets:\n"
            + "+------+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "|test.a|count(distinct test.b)|avg(distinct test.b)|sum(distinct test.b)|min(distinct test.b)|max(distinct test.b)|\n"
            + "+------+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "|     1|                     1|                 1.0|                   1|                   1|                   1|\n"
            + "|     2|                     2|                 1.5|                   3|                   1|                   2|\n"
            + "|     3|                     2|                 1.5|                   3|                   1|                   2|\n"
            + "|     4|                     3|                 2.0|                   6|                   1|                   3|\n"
            + "+------+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT COUNT(a), AVG(a), SUM(a), MIN(a), MAX(a) FROM test OVER (RANGE 2 IN (0, 10]);";
    expected =
        "ResultSets:\n"
            + "+---+-------------+-----------+-----------+-----------+-----------+\n"
            + "|key|count(test.a)|avg(test.a)|sum(test.a)|min(test.a)|max(test.a)|\n"
            + "+---+-------------+-----------+-----------+-----------+-----------+\n"
            + "|  1|            2|        1.5|          3|          1|          2|\n"
            + "|  3|            2|        2.5|          5|          2|          3|\n"
            + "|  5|            2|        3.0|          6|          3|          3|\n"
            + "|  7|            2|        4.0|          8|          4|          4|\n"
            + "|  9|            2|        4.0|          8|          4|          4|\n"
            + "+---+-------------+-----------+-----------+-----------+-----------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT COUNT(DISTINCT a), AVG(DISTINCT a), SUM(DISTINCT a), MIN(DISTINCT a), MAX(DISTINCT a) FROM test OVER (RANGE 2 IN (0, 10]);";
    expected =
        "ResultSets:\n"
            + "+---+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "|key|count(distinct test.a)|avg(distinct test.a)|sum(distinct test.a)|min(distinct test.a)|max(distinct test.a)|\n"
            + "+---+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "|  1|                     2|                 1.5|                   3|                   1|                   2|\n"
            + "|  3|                     2|                 2.5|                   5|                   2|                   3|\n"
            + "|  5|                     1|                 3.0|                   3|                   3|                   3|\n"
            + "|  7|                     1|                 4.0|                   4|                   4|                   4|\n"
            + "|  9|                     1|                 4.0|                   4|                   4|                   4|\n"
            + "+---+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testLimitAndOffsetQuery() {
    String statement = "SELECT s1 FROM us.d1 WHERE key > 0 AND key < 10000 limit 10;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s1|\n"
            + "+---+--------+\n"
            + "|  1|       1|\n"
            + "|  2|       2|\n"
            + "|  3|       3|\n"
            + "|  4|       4|\n"
            + "|  5|       5|\n"
            + "|  6|       6|\n"
            + "|  7|       7|\n"
            + "|  8|       8|\n"
            + "|  9|       9|\n"
            + "| 10|      10|\n"
            + "+---+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT s1 FROM us.d1 WHERE key > 0 AND key < 10000 limit 10 offset 5;";
    expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s1|\n"
            + "+---+--------+\n"
            + "|  6|       6|\n"
            + "|  7|       7|\n"
            + "|  8|       8|\n"
            + "|  9|       9|\n"
            + "| 10|      10|\n"
            + "| 11|      11|\n"
            + "| 12|      12|\n"
            + "| 13|      13|\n"
            + "| 14|      14|\n"
            + "| 15|      15|\n"
            + "+---+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testOrderByQuery() {
    String insert =
        "INSERT INTO us.d2 (key, s1, s2, s3) values "
            + "(1, \"apple\", 871, 232.1), (2, \"peach\", 123, 132.5), (3, \"banana\", 356, 317.8),"
            + "(4, \"cherry\", 621, 456.1), (5, \"grape\", 336, 132.5), (6, \"dates\", 119, 232.1),"
            + "(7, \"melon\", 516, 113.6), (8, \"mango\", 458, 232.1), (9, \"pear\", 336, 613.1);";
    executor.execute(insert);

    String orderByQuery = "SELECT * FROM us.d2 ORDER BY KEY";
    String expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|us.d2.s1|us.d2.s2|us.d2.s3|\n"
            + "+---+--------+--------+--------+\n"
            + "|  1|   apple|     871|   232.1|\n"
            + "|  2|   peach|     123|   132.5|\n"
            + "|  3|  banana|     356|   317.8|\n"
            + "|  4|  cherry|     621|   456.1|\n"
            + "|  5|   grape|     336|   132.5|\n"
            + "|  6|   dates|     119|   232.1|\n"
            + "|  7|   melon|     516|   113.6|\n"
            + "|  8|   mango|     458|   232.1|\n"
            + "|  9|    pear|     336|   613.1|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(orderByQuery, expected);

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s1";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|us.d2.s1|us.d2.s2|us.d2.s3|\n"
            + "+---+--------+--------+--------+\n"
            + "|  1|   apple|     871|   232.1|\n"
            + "|  3|  banana|     356|   317.8|\n"
            + "|  4|  cherry|     621|   456.1|\n"
            + "|  6|   dates|     119|   232.1|\n"
            + "|  5|   grape|     336|   132.5|\n"
            + "|  8|   mango|     458|   232.1|\n"
            + "|  7|   melon|     516|   113.6|\n"
            + "|  2|   peach|     123|   132.5|\n"
            + "|  9|    pear|     336|   613.1|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(orderByQuery, expected);

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s1 DESC";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|us.d2.s1|us.d2.s2|us.d2.s3|\n"
            + "+---+--------+--------+--------+\n"
            + "|  9|    pear|     336|   613.1|\n"
            + "|  2|   peach|     123|   132.5|\n"
            + "|  7|   melon|     516|   113.6|\n"
            + "|  8|   mango|     458|   232.1|\n"
            + "|  5|   grape|     336|   132.5|\n"
            + "|  6|   dates|     119|   232.1|\n"
            + "|  4|  cherry|     621|   456.1|\n"
            + "|  3|  banana|     356|   317.8|\n"
            + "|  1|   apple|     871|   232.1|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(orderByQuery, expected);

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s3";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|us.d2.s1|us.d2.s2|us.d2.s3|\n"
            + "+---+--------+--------+--------+\n"
            + "|  7|   melon|     516|   113.6|\n"
            + "|  2|   peach|     123|   132.5|\n"
            + "|  5|   grape|     336|   132.5|\n"
            + "|  1|   apple|     871|   232.1|\n"
            + "|  6|   dates|     119|   232.1|\n"
            + "|  8|   mango|     458|   232.1|\n"
            + "|  3|  banana|     356|   317.8|\n"
            + "|  4|  cherry|     621|   456.1|\n"
            + "|  9|    pear|     336|   613.1|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(orderByQuery, expected);

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s3, s2";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|us.d2.s1|us.d2.s2|us.d2.s3|\n"
            + "+---+--------+--------+--------+\n"
            + "|  7|   melon|     516|   113.6|\n"
            + "|  2|   peach|     123|   132.5|\n"
            + "|  5|   grape|     336|   132.5|\n"
            + "|  6|   dates|     119|   232.1|\n"
            + "|  8|   mango|     458|   232.1|\n"
            + "|  1|   apple|     871|   232.1|\n"
            + "|  3|  banana|     356|   317.8|\n"
            + "|  4|  cherry|     621|   456.1|\n"
            + "|  9|    pear|     336|   613.1|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(orderByQuery, expected);

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s3, s2 DESC";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|us.d2.s1|us.d2.s2|us.d2.s3|\n"
            + "+---+--------+--------+--------+\n"
            + "|  9|    pear|     336|   613.1|\n"
            + "|  4|  cherry|     621|   456.1|\n"
            + "|  3|  banana|     356|   317.8|\n"
            + "|  1|   apple|     871|   232.1|\n"
            + "|  8|   mango|     458|   232.1|\n"
            + "|  6|   dates|     119|   232.1|\n"
            + "|  5|   grape|     336|   132.5|\n"
            + "|  2|   peach|     123|   132.5|\n"
            + "|  7|   melon|     516|   113.6|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(orderByQuery, expected);
  }

  @Test
  public void testRowTransformFunction() {
    String insert =
        "INSERT INTO us.d2 (key, s1, s2, s3) values "
            + "(1, \"apple\", 871, 232.1), (2, \"peach\", 123, 132.5), (3, \"banana\", 356, 317.8),"
            + "(4, \"cherry\", 621, 456.1), (5, \"grape\", 336, 132.5), (6, \"dates\", 119, 232.1),"
            + "(7, \"melon\", 516, 113.6), (8, \"mango\", 458, 232.1), (9, \"pear\", 336, 613.1);";
    executor.execute(insert);

    String statement = "SELECT RATIO(s2, s3) FROM us.d2;";
    String expected =
        "ResultSets:\n"
            + "+---+-------------------------+\n"
            + "|key|ratio(us.d2.s2, us.d2.s3)|\n"
            + "+---+-------------------------+\n"
            + "|  1|       3.7526928048255064|\n"
            + "|  2|       0.9283018867924528|\n"
            + "|  3|        1.120201384518565|\n"
            + "|  4|       1.3615435211576408|\n"
            + "|  5|       2.5358490566037735|\n"
            + "|  6|       0.5127100387763895|\n"
            + "|  7|        4.542253521126761|\n"
            + "|  8|       1.9732873761309782|\n"
            + "|  9|       0.5480345783722068|\n"
            + "+---+-------------------------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT RATIO(s2, s3) FROM us.d2 WHERE key <= 6;";
    expected =
        "ResultSets:\n"
            + "+---+-------------------------+\n"
            + "|key|ratio(us.d2.s2, us.d2.s3)|\n"
            + "+---+-------------------------+\n"
            + "|  1|       3.7526928048255064|\n"
            + "|  2|       0.9283018867924528|\n"
            + "|  3|        1.120201384518565|\n"
            + "|  4|       1.3615435211576408|\n"
            + "|  5|       2.5358490566037735|\n"
            + "|  6|       0.5127100387763895|\n"
            + "+---+-------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testFirstLastQuery() {
    String statement = "SELECT FIRST(s2) FROM us.d1 WHERE key > 0;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+-----+\n"
            + "|key|    path|value|\n"
            + "+---+--------+-----+\n"
            + "|  1|us.d1.s2|    2|\n"
            + "+---+--------+-----+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT LAST(s2) FROM us.d1 WHERE key > 0;";
    expected =
        "ResultSets:\n"
            + "+-----+--------+-----+\n"
            + "|  key|    path|value|\n"
            + "+-----+--------+-----+\n"
            + "|14999|us.d1.s2|15000|\n"
            + "+-----+--------+-----+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT FIRST(s4) FROM us.d1 WHERE key > 0;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-----+\n"
            + "|key|    path|value|\n"
            + "+---+--------+-----+\n"
            + "|  1|us.d1.s4|  1.1|\n"
            + "+---+--------+-----+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT LAST(s4) FROM us.d1 WHERE key > 0;";
    expected =
        "ResultSets:\n"
            + "+-----+--------+-------+\n"
            + "|  key|    path|  value|\n"
            + "+-----+--------+-------+\n"
            + "|14999|us.d1.s4|14999.1|\n"
            + "+-----+--------+-------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT LAST(s2), LAST(s4) FROM us.d1 WHERE key > 0;";
    expected =
        "ResultSets:\n"
            + "+-----+--------+-------+\n"
            + "|  key|    path|  value|\n"
            + "+-----+--------+-------+\n"
            + "|14999|us.d1.s2|  15000|\n"
            + "|14999|us.d1.s4|14999.1|\n"
            + "+-----+--------+-------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT FIRST(s2), FIRST(s4) FROM us.d1 WHERE key > 0;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-----+\n"
            + "|key|    path|value|\n"
            + "+---+--------+-----+\n"
            + "|  1|us.d1.s2|    2|\n"
            + "|  1|us.d1.s4|  1.1|\n"
            + "+---+--------+-----+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT LAST(s2), LAST(s4) FROM us.d1 WHERE key < 1000;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-----+\n"
            + "|key|    path|value|\n"
            + "+---+--------+-----+\n"
            + "|999|us.d1.s2| 1000|\n"
            + "|999|us.d1.s4|999.1|\n"
            + "+---+--------+-----+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT FIRST(s2), LAST(s4) FROM us.d1 WHERE key > 1000;";
    expected =
        "ResultSets:\n"
            + "+-----+--------+-------+\n"
            + "|  key|    path|  value|\n"
            + "+-----+--------+-------+\n"
            + "| 1001|us.d1.s2|   1002|\n"
            + "|14999|us.d1.s4|14999.1|\n"
            + "+-----+--------+-------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT FIRST(s4), LAST(s2) FROM us.d1 WHERE key > 1000;";
    expected =
        "ResultSets:\n"
            + "+-----+--------+------+\n"
            + "|  key|    path| value|\n"
            + "+-----+--------+------+\n"
            + "| 1001|us.d1.s4|1001.1|\n"
            + "|14999|us.d1.s2| 15000|\n"
            + "+-----+--------+------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT FIRST(s2), LAST(s2) FROM us.d1 WHERE key > 1000;";
    expected =
        "ResultSets:\n"
            + "+-----+--------+-----+\n"
            + "|  key|    path|value|\n"
            + "+-----+--------+-----+\n"
            + "| 1001|us.d1.s2| 1002|\n"
            + "|14999|us.d1.s2|15000|\n"
            + "+-----+--------+-----+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT FIRST(s4), LAST(s4) FROM us.d1 WHERE key > 1000;";
    expected =
        "ResultSets:\n"
            + "+-----+--------+-------+\n"
            + "|  key|    path|  value|\n"
            + "+-----+--------+-------+\n"
            + "| 1001|us.d1.s4| 1001.1|\n"
            + "|14999|us.d1.s4|14999.1|\n"
            + "+-----+--------+-------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testAggregateQuery() {
    String statement = "SELECT %s(s1), %s(s2) FROM us.d1 WHERE key > 0 AND key < 1000;";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+-------------+-------------+\n"
                + "|max(us.d1.s1)|max(us.d1.s2)|\n"
                + "+-------------+-------------+\n"
                + "|          999|         1000|\n"
                + "+-------------+-------------+\n"
                + "Total line number = 1\n",
            "ResultSets:\n"
                + "+-------------+-------------+\n"
                + "|min(us.d1.s1)|min(us.d1.s2)|\n"
                + "+-------------+-------------+\n"
                + "|            1|            2|\n"
                + "+-------------+-------------+\n"
                + "Total line number = 1\n",
            "ResultSets:\n"
                + "+---------------------+---------------------+\n"
                + "|first_value(us.d1.s1)|first_value(us.d1.s2)|\n"
                + "+---------------------+---------------------+\n"
                + "|                    1|                    2|\n"
                + "+---------------------+---------------------+\n"
                + "Total line number = 1\n",
            "ResultSets:\n"
                + "+--------------------+--------------------+\n"
                + "|last_value(us.d1.s1)|last_value(us.d1.s2)|\n"
                + "+--------------------+--------------------+\n"
                + "|                 999|                1000|\n"
                + "+--------------------+--------------------+\n"
                + "Total line number = 1\n",
            "ResultSets:\n"
                + "+-------------+-------------+\n"
                + "|sum(us.d1.s1)|sum(us.d1.s2)|\n"
                + "+-------------+-------------+\n"
                + "|       499500|       500499|\n"
                + "+-------------+-------------+\n"
                + "Total line number = 1\n",
            "ResultSets:\n"
                + "+-------------+-------------+\n"
                + "|avg(us.d1.s1)|avg(us.d1.s2)|\n"
                + "+-------------+-------------+\n"
                + "|        500.0|        501.0|\n"
                + "+-------------+-------------+\n"
                + "Total line number = 1\n",
            "ResultSets:\n"
                + "+---------------+---------------+\n"
                + "|count(us.d1.s1)|count(us.d1.s2)|\n"
                + "+---------------+---------------+\n"
                + "|            999|            999|\n"
                + "+---------------+---------------+\n"
                + "Total line number = 1\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }
  }

  @Test
  public void testDownSampleQuery() {
    String statement = "SELECT %s(s1), %s(s4) FROM us.d1 OVER (RANGE 100 IN (0, 1000));";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+-------------+-------------+\n"
                + "|key|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+-------------+-------------+\n"
                + "|  1|          100|        100.1|\n"
                + "|101|          200|        200.1|\n"
                + "|201|          300|        300.1|\n"
                + "|301|          400|        400.1|\n"
                + "|401|          500|        500.1|\n"
                + "|501|          600|        600.1|\n"
                + "|601|          700|        700.1|\n"
                + "|701|          800|        800.1|\n"
                + "|801|          900|        900.1|\n"
                + "|901|          999|        999.1|\n"
                + "+---+-------------+-------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+-------------+-------------+\n"
                + "|key|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+-------------+-------------+\n"
                + "|  1|            1|          1.1|\n"
                + "|101|          101|        101.1|\n"
                + "|201|          201|        201.1|\n"
                + "|301|          301|        301.1|\n"
                + "|401|          401|        401.1|\n"
                + "|501|          501|        501.1|\n"
                + "|601|          601|        601.1|\n"
                + "|701|          701|        701.1|\n"
                + "|801|          801|        801.1|\n"
                + "|901|          901|        901.1|\n"
                + "+---+-------------+-------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+---------------------+---------------------+\n"
                + "|key|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+---------------------+---------------------+\n"
                + "|  1|                    1|                  1.1|\n"
                + "|101|                  101|                101.1|\n"
                + "|201|                  201|                201.1|\n"
                + "|301|                  301|                301.1|\n"
                + "|401|                  401|                401.1|\n"
                + "|501|                  501|                501.1|\n"
                + "|601|                  601|                601.1|\n"
                + "|701|                  701|                701.1|\n"
                + "|801|                  801|                801.1|\n"
                + "|901|                  901|                901.1|\n"
                + "+---+---------------------+---------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+--------------------+--------------------+\n"
                + "|key|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+--------------------+--------------------+\n"
                + "|  1|                 100|               100.1|\n"
                + "|101|                 200|               200.1|\n"
                + "|201|                 300|               300.1|\n"
                + "|301|                 400|               400.1|\n"
                + "|401|                 500|               500.1|\n"
                + "|501|                 600|               600.1|\n"
                + "|601|                 700|               700.1|\n"
                + "|701|                 800|               800.1|\n"
                + "|801|                 900|               900.1|\n"
                + "|901|                 999|               999.1|\n"
                + "+---+--------------------+--------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+-------------+------------------+\n"
                + "|key|sum(us.d1.s1)|     sum(us.d1.s4)|\n"
                + "+---+-------------+------------------+\n"
                + "|  1|         5050|            5060.0|\n"
                + "|101|        15050|15060.000000000022|\n"
                + "|201|        25050| 25059.99999999997|\n"
                + "|301|        35050| 35059.99999999994|\n"
                + "|401|        45050| 45059.99999999992|\n"
                + "|501|        55050| 55059.99999999991|\n"
                + "|601|        65050|  65059.9999999999|\n"
                + "|701|        75050| 75059.99999999999|\n"
                + "|801|        85050| 85060.00000000004|\n"
                + "|901|        94050|  94059.9000000001|\n"
                + "+---+-------------+------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+-------------+------------------+\n"
                + "|key|avg(us.d1.s1)|     avg(us.d1.s4)|\n"
                + "+---+-------------+------------------+\n"
                + "|  1|         50.5|              50.6|\n"
                + "|101|        150.5|150.60000000000022|\n"
                + "|201|        250.5| 250.5999999999997|\n"
                + "|301|        350.5| 350.5999999999994|\n"
                + "|401|        450.5| 450.5999999999992|\n"
                + "|501|        550.5| 550.5999999999991|\n"
                + "|601|        650.5|  650.599999999999|\n"
                + "|701|        750.5| 750.5999999999999|\n"
                + "|801|        850.5| 850.6000000000005|\n"
                + "|901|        950.0| 950.1000000000009|\n"
                + "+---+-------------+------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+---------------+---------------+\n"
                + "|key|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+---------------+---------------+\n"
                + "|  1|            100|            100|\n"
                + "|101|            100|            100|\n"
                + "|201|            100|            100|\n"
                + "|301|            100|            100|\n"
                + "|401|            100|            100|\n"
                + "|501|            100|            100|\n"
                + "|601|            100|            100|\n"
                + "|701|            100|            100|\n"
                + "|801|            100|            100|\n"
                + "|901|             99|             99|\n"
                + "+---+---------------+---------------+\n"
                + "Total line number = 10\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }
  }

  @Test
  public void testRangeDownSampleQuery() {
    String statement =
        "SELECT %s(s1), %s(s4) FROM us.d1 WHERE key > 600 AND s1 <= 900 OVER (RANGE 100 IN (0, 1000));";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+-------------+-------------+\n"
                + "|key|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+-------------+-------------+\n"
                + "|601|          700|        700.1|\n"
                + "|701|          800|        800.1|\n"
                + "|801|          900|        900.1|\n"
                + "+---+-------------+-------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+-------------+-------------+\n"
                + "|key|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+-------------+-------------+\n"
                + "|601|          601|        601.1|\n"
                + "|701|          701|        701.1|\n"
                + "|801|          801|        801.1|\n"
                + "+---+-------------+-------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+---------------------+---------------------+\n"
                + "|key|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+---------------------+---------------------+\n"
                + "|601|                  601|                601.1|\n"
                + "|701|                  701|                701.1|\n"
                + "|801|                  801|                801.1|\n"
                + "+---+---------------------+---------------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+--------------------+--------------------+\n"
                + "|key|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+--------------------+--------------------+\n"
                + "|601|                 700|               700.1|\n"
                + "|701|                 800|               800.1|\n"
                + "|801|                 900|               900.1|\n"
                + "+---+--------------------+--------------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+-------------+-----------------+\n"
                + "|key|sum(us.d1.s1)|    sum(us.d1.s4)|\n"
                + "+---+-------------+-----------------+\n"
                + "|601|        65050| 65059.9999999999|\n"
                + "|701|        75050|75059.99999999999|\n"
                + "|801|        85050|85060.00000000004|\n"
                + "+---+-------------+-----------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+-------------+-----------------+\n"
                + "|key|avg(us.d1.s1)|    avg(us.d1.s4)|\n"
                + "+---+-------------+-----------------+\n"
                + "|601|        650.5| 650.599999999999|\n"
                + "|701|        750.5|750.5999999999999|\n"
                + "|801|        850.5|850.6000000000005|\n"
                + "+---+-------------+-----------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+---------------+---------------+\n"
                + "|key|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+---------------+---------------+\n"
                + "|601|            100|            100|\n"
                + "|701|            100|            100|\n"
                + "|801|            100|            100|\n"
                + "+---+---------------+---------------+\n"
                + "Total line number = 3\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }
  }

  @Test
  public void testSlideWindowByTimeQuery() {
    String statement = "SELECT %s(s1), %s(s4) FROM us.d1 OVER (RANGE 100 IN (0, 1000) STEP 50);";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+-------------+-------------+\n"
                + "|key|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+-------------+-------------+\n"
                + "|  1|          100|        100.1|\n"
                + "| 51|          150|        150.1|\n"
                + "|101|          200|        200.1|\n"
                + "|151|          250|        250.1|\n"
                + "|201|          300|        300.1|\n"
                + "|251|          350|        350.1|\n"
                + "|301|          400|        400.1|\n"
                + "|351|          450|        450.1|\n"
                + "|401|          500|        500.1|\n"
                + "|451|          550|        550.1|\n"
                + "|501|          600|        600.1|\n"
                + "|551|          650|        650.1|\n"
                + "|601|          700|        700.1|\n"
                + "|651|          750|        750.1|\n"
                + "|701|          800|        800.1|\n"
                + "|751|          850|        850.1|\n"
                + "|801|          900|        900.1|\n"
                + "|851|          950|        950.1|\n"
                + "|901|          999|        999.1|\n"
                + "+---+-------------+-------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+-------------+-------------+\n"
                + "|key|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+-------------+-------------+\n"
                + "|  1|            1|          1.1|\n"
                + "| 51|           51|         51.1|\n"
                + "|101|          101|        101.1|\n"
                + "|151|          151|        151.1|\n"
                + "|201|          201|        201.1|\n"
                + "|251|          251|        251.1|\n"
                + "|301|          301|        301.1|\n"
                + "|351|          351|        351.1|\n"
                + "|401|          401|        401.1|\n"
                + "|451|          451|        451.1|\n"
                + "|501|          501|        501.1|\n"
                + "|551|          551|        551.1|\n"
                + "|601|          601|        601.1|\n"
                + "|651|          651|        651.1|\n"
                + "|701|          701|        701.1|\n"
                + "|751|          751|        751.1|\n"
                + "|801|          801|        801.1|\n"
                + "|851|          851|        851.1|\n"
                + "|901|          901|        901.1|\n"
                + "+---+-------------+-------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+---------------------+---------------------+\n"
                + "|key|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+---------------------+---------------------+\n"
                + "|  1|                    1|                  1.1|\n"
                + "| 51|                   51|                 51.1|\n"
                + "|101|                  101|                101.1|\n"
                + "|151|                  151|                151.1|\n"
                + "|201|                  201|                201.1|\n"
                + "|251|                  251|                251.1|\n"
                + "|301|                  301|                301.1|\n"
                + "|351|                  351|                351.1|\n"
                + "|401|                  401|                401.1|\n"
                + "|451|                  451|                451.1|\n"
                + "|501|                  501|                501.1|\n"
                + "|551|                  551|                551.1|\n"
                + "|601|                  601|                601.1|\n"
                + "|651|                  651|                651.1|\n"
                + "|701|                  701|                701.1|\n"
                + "|751|                  751|                751.1|\n"
                + "|801|                  801|                801.1|\n"
                + "|851|                  851|                851.1|\n"
                + "|901|                  901|                901.1|\n"
                + "+---+---------------------+---------------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+--------------------+--------------------+\n"
                + "|key|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+--------------------+--------------------+\n"
                + "|  1|                 100|               100.1|\n"
                + "| 51|                 150|               150.1|\n"
                + "|101|                 200|               200.1|\n"
                + "|151|                 250|               250.1|\n"
                + "|201|                 300|               300.1|\n"
                + "|251|                 350|               350.1|\n"
                + "|301|                 400|               400.1|\n"
                + "|351|                 450|               450.1|\n"
                + "|401|                 500|               500.1|\n"
                + "|451|                 550|               550.1|\n"
                + "|501|                 600|               600.1|\n"
                + "|551|                 650|               650.1|\n"
                + "|601|                 700|               700.1|\n"
                + "|651|                 750|               750.1|\n"
                + "|701|                 800|               800.1|\n"
                + "|751|                 850|               850.1|\n"
                + "|801|                 900|               900.1|\n"
                + "|851|                 950|               950.1|\n"
                + "|901|                 999|               999.1|\n"
                + "+---+--------------------+--------------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+-------------+------------------+\n"
                + "|key|sum(us.d1.s1)|     sum(us.d1.s4)|\n"
                + "+---+-------------+------------------+\n"
                + "|  1|         5050|            5060.0|\n"
                + "| 51|        10050|10060.000000000013|\n"
                + "|101|        15050|15060.000000000022|\n"
                + "|151|        20050|20059.999999999996|\n"
                + "|201|        25050| 25059.99999999997|\n"
                + "|251|        30050|30059.999999999953|\n"
                + "|301|        35050| 35059.99999999994|\n"
                + "|351|        40050| 40059.99999999993|\n"
                + "|401|        45050| 45059.99999999992|\n"
                + "|451|        50050| 50059.99999999992|\n"
                + "|501|        55050| 55059.99999999991|\n"
                + "|551|        60050|60059.999999999905|\n"
                + "|601|        65050|  65059.9999999999|\n"
                + "|651|        70050| 70059.99999999994|\n"
                + "|701|        75050| 75059.99999999999|\n"
                + "|751|        80050| 80060.00000000001|\n"
                + "|801|        85050| 85060.00000000004|\n"
                + "|851|        90050| 90060.00000000009|\n"
                + "|901|        94050|  94059.9000000001|\n"
                + "+---+-------------+------------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+-------------+------------------+\n"
                + "|key|avg(us.d1.s1)|     avg(us.d1.s4)|\n"
                + "+---+-------------+------------------+\n"
                + "|  1|         50.5|              50.6|\n"
                + "| 51|        100.5|100.60000000000012|\n"
                + "|101|        150.5|150.60000000000022|\n"
                + "|151|        200.5|200.59999999999997|\n"
                + "|201|        250.5| 250.5999999999997|\n"
                + "|251|        300.5| 300.5999999999995|\n"
                + "|301|        350.5| 350.5999999999994|\n"
                + "|351|        400.5| 400.5999999999993|\n"
                + "|401|        450.5| 450.5999999999992|\n"
                + "|451|        500.5| 500.5999999999992|\n"
                + "|501|        550.5| 550.5999999999991|\n"
                + "|551|        600.5|  600.599999999999|\n"
                + "|601|        650.5|  650.599999999999|\n"
                + "|651|        700.5| 700.5999999999995|\n"
                + "|701|        750.5| 750.5999999999999|\n"
                + "|751|        800.5| 800.6000000000001|\n"
                + "|801|        850.5| 850.6000000000005|\n"
                + "|851|        900.5| 900.6000000000008|\n"
                + "|901|        950.0| 950.1000000000009|\n"
                + "+---+-------------+------------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+---------------+---------------+\n"
                + "|key|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+---------------+---------------+\n"
                + "|  1|            100|            100|\n"
                + "| 51|            100|            100|\n"
                + "|101|            100|            100|\n"
                + "|151|            100|            100|\n"
                + "|201|            100|            100|\n"
                + "|251|            100|            100|\n"
                + "|301|            100|            100|\n"
                + "|351|            100|            100|\n"
                + "|401|            100|            100|\n"
                + "|451|            100|            100|\n"
                + "|501|            100|            100|\n"
                + "|551|            100|            100|\n"
                + "|601|            100|            100|\n"
                + "|651|            100|            100|\n"
                + "|701|            100|            100|\n"
                + "|751|            100|            100|\n"
                + "|801|            100|            100|\n"
                + "|851|            100|            100|\n"
                + "|901|             99|             99|\n"
                + "+---+---------------+---------------+\n"
                + "Total line number = 19\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }
  }

  @Test
  public void testRangeSlideWindowByTimeQuery() {
    String statement =
        "SELECT %s(s1), %s(s4) FROM us.d1 WHERE key > 300 AND s1 <= 600 OVER (RANGE 100 IN (0, 1000) STEP 50);";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+-------------+-------------+\n"
                + "|key|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+-------------+-------------+\n"
                + "|251|          350|        350.1|\n"
                + "|301|          400|        400.1|\n"
                + "|351|          450|        450.1|\n"
                + "|401|          500|        500.1|\n"
                + "|451|          550|        550.1|\n"
                + "|501|          600|        600.1|\n"
                + "|551|          600|        600.1|\n"
                + "+---+-------------+-------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+-------------+-------------+\n"
                + "|key|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+-------------+-------------+\n"
                + "|251|          301|        301.1|\n"
                + "|301|          301|        301.1|\n"
                + "|351|          351|        351.1|\n"
                + "|401|          401|        401.1|\n"
                + "|451|          451|        451.1|\n"
                + "|501|          501|        501.1|\n"
                + "|551|          551|        551.1|\n"
                + "+---+-------------+-------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+---------------------+---------------------+\n"
                + "|key|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+---------------------+---------------------+\n"
                + "|251|                  301|                301.1|\n"
                + "|301|                  301|                301.1|\n"
                + "|351|                  351|                351.1|\n"
                + "|401|                  401|                401.1|\n"
                + "|451|                  451|                451.1|\n"
                + "|501|                  501|                501.1|\n"
                + "|551|                  551|                551.1|\n"
                + "+---+---------------------+---------------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+--------------------+--------------------+\n"
                + "|key|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+--------------------+--------------------+\n"
                + "|251|                 350|               350.1|\n"
                + "|301|                 400|               400.1|\n"
                + "|351|                 450|               450.1|\n"
                + "|401|                 500|               500.1|\n"
                + "|451|                 550|               550.1|\n"
                + "|501|                 600|               600.1|\n"
                + "|551|                 600|               600.1|\n"
                + "+---+--------------------+--------------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+-------------+------------------+\n"
                + "|key|sum(us.d1.s1)|     sum(us.d1.s4)|\n"
                + "+---+-------------+------------------+\n"
                + "|251|        16275|16280.000000000013|\n"
                + "|301|        35050| 35059.99999999994|\n"
                + "|351|        40050| 40059.99999999993|\n"
                + "|401|        45050| 45059.99999999992|\n"
                + "|451|        50050| 50059.99999999992|\n"
                + "|501|        55050| 55059.99999999991|\n"
                + "|551|        28775|28779.999999999975|\n"
                + "+---+-------------+------------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+-------------+------------------+\n"
                + "|key|avg(us.d1.s1)|     avg(us.d1.s4)|\n"
                + "+---+-------------+------------------+\n"
                + "|251|        325.5|325.60000000000025|\n"
                + "|301|        350.5| 350.5999999999994|\n"
                + "|351|        400.5| 400.5999999999993|\n"
                + "|401|        450.5| 450.5999999999992|\n"
                + "|451|        500.5| 500.5999999999992|\n"
                + "|501|        550.5| 550.5999999999991|\n"
                + "|551|        575.5| 575.5999999999995|\n"
                + "+---+-------------+------------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+---------------+---------------+\n"
                + "|key|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+---------------+---------------+\n"
                + "|251|             50|             50|\n"
                + "|301|            100|            100|\n"
                + "|351|            100|            100|\n"
                + "|401|            100|            100|\n"
                + "|451|            100|            100|\n"
                + "|501|            100|            100|\n"
                + "|551|             50|             50|\n"
                + "+---+---------------+---------------+\n"
                + "Total line number = 7\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }
  }

  @Test
  public void testAggregateWithLevel() {
    String insert =
        "INSERT INTO test(key, a1.b1, a1.b2, a2.b1, a2.b2) VALUES (0, 0, 0, 0, 0), (1, 1, 1, 1, 1),"
            + "(2, NULL, 2, 2, 2), (3, NULL, NULL, 3, 3), (4, NULL, NULL, NULL, 4)";
    executor.execute(insert);

    String statement = "SELECT AVG(*), COUNT(*), SUM(*) FROM test AGG LEVEL = 0;";
    String expected =
        "ResultSets:\n"
            + "+------------------+---------------+-------------+\n"
            + "|     avg(test.*.*)|count(test.*.*)|sum(test.*.*)|\n"
            + "+------------------+---------------+-------------+\n"
            + "|1.4285714285714286|             14|           20|\n"
            + "+------------------+---------------+-------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT AVG(*), COUNT(*), SUM(*) FROM test AGG LEVEL = 0,1;";
    expected =
        "ResultSets:\n"
            + "+--------------+------------------+----------------+----------------+--------------+--------------+\n"
            + "|avg(test.a1.*)|    avg(test.a2.*)|count(test.a1.*)|count(test.a2.*)|sum(test.a1.*)|sum(test.a2.*)|\n"
            + "+--------------+------------------+----------------+----------------+--------------+--------------+\n"
            + "|           0.8|1.7777777777777777|               5|               9|             4|            16|\n"
            + "+--------------+------------------+----------------+----------------+--------------+--------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT AVG(*), COUNT(*), SUM(*) FROM test AGG LEVEL = 1;";
    expected =
        "ResultSets:\n"
            + "+-----------+------------------+-------------+-------------+-----------+-----------+\n"
            + "|avg(*.a1.*)|       avg(*.a2.*)|count(*.a1.*)|count(*.a2.*)|sum(*.a1.*)|sum(*.a2.*)|\n"
            + "+-----------+------------------+-------------+-------------+-----------+-----------+\n"
            + "|        0.8|1.7777777777777777|            5|            9|          4|         16|\n"
            + "+-----------+------------------+-------------+-------------+-----------+-----------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT SUM(*), COUNT(*), AVG(*) FROM test AGG LEVEL = 0,2;";
    expected =
        "ResultSets:\n"
            + "+--------------+--------------+----------------+----------------+------------------+--------------+\n"
            + "|sum(test.*.b1)|sum(test.*.b2)|count(test.*.b1)|count(test.*.b2)|    avg(test.*.b1)|avg(test.*.b2)|\n"
            + "+--------------+--------------+----------------+----------------+------------------+--------------+\n"
            + "|             7|            13|               6|               8|1.1666666666666667|         1.625|\n"
            + "+--------------+--------------+----------------+----------------+------------------+--------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT SUM(*), COUNT(*), AVG(*) FROM test AGG LEVEL = 2;";
    expected =
        "ResultSets:\n"
            + "+-----------+-----------+-------------+-------------+------------------+-----------+\n"
            + "|sum(*.*.b1)|sum(*.*.b2)|count(*.*.b1)|count(*.*.b2)|       avg(*.*.b1)|avg(*.*.b2)|\n"
            + "+-----------+-----------+-------------+-------------+------------------+-----------+\n"
            + "|          7|         13|            6|            8|1.1666666666666667|      1.625|\n"
            + "+-----------+-----------+-------------+-------------+------------------+-----------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testDelete() {
    if (!isAbleToDelete) {
      return;
    }
    String delete = "DELETE FROM us.d1.s1 WHERE key > 105 AND key < 115;";
    executor.execute(delete);

    String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE key > 100 AND key < 120;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s1|\n"
            + "+---+--------+\n"
            + "|101|     101|\n"
            + "|102|     102|\n"
            + "|103|     103|\n"
            + "|104|     104|\n"
            + "|105|     105|\n"
            + "|115|     115|\n"
            + "|116|     116|\n"
            + "|117|     117|\n"
            + "|118|     118|\n"
            + "|119|     119|\n"
            + "+---+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(queryOverDeleteRange, expected);

    delete = "DELETE FROM us.d1.s1 WHERE key >= 1126 AND key <= 1155;";
    executor.execute(delete);

    queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE key > 1120 AND key < 1160;";
    expected =
        "ResultSets:\n"
            + "+----+--------+\n"
            + "| key|us.d1.s1|\n"
            + "+----+--------+\n"
            + "|1121|    1121|\n"
            + "|1122|    1122|\n"
            + "|1123|    1123|\n"
            + "|1124|    1124|\n"
            + "|1125|    1125|\n"
            + "|1156|    1156|\n"
            + "|1157|    1157|\n"
            + "|1158|    1158|\n"
            + "|1159|    1159|\n"
            + "+----+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(queryOverDeleteRange, expected);

    delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE key > 2236 AND key <= 2265;";
    executor.execute(delete);

    queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE key > 2230 AND key < 2270;";
    expected =
        "ResultSets:\n"
            + "+----+--------+--------+\n"
            + "| key|us.d1.s2|us.d1.s4|\n"
            + "+----+--------+--------+\n"
            + "|2231|    2232|  2231.1|\n"
            + "|2232|    2233|  2232.1|\n"
            + "|2233|    2234|  2233.1|\n"
            + "|2234|    2235|  2234.1|\n"
            + "|2235|    2236|  2235.1|\n"
            + "|2236|    2237|  2236.1|\n"
            + "|2266|    2267|  2266.1|\n"
            + "|2267|    2268|  2267.1|\n"
            + "|2268|    2269|  2268.1|\n"
            + "|2269|    2270|  2269.1|\n"
            + "+----+--------+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(queryOverDeleteRange, expected);

    delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE key >= 3346 AND key < 3375;";
    executor.execute(delete);

    queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE key > 3340 AND key < 3380;";
    expected =
        "ResultSets:\n"
            + "+----+--------+--------+\n"
            + "| key|us.d1.s2|us.d1.s4|\n"
            + "+----+--------+--------+\n"
            + "|3341|    3342|  3341.1|\n"
            + "|3342|    3343|  3342.1|\n"
            + "|3343|    3344|  3343.1|\n"
            + "|3344|    3345|  3344.1|\n"
            + "|3345|    3346|  3345.1|\n"
            + "|3375|    3376|  3375.1|\n"
            + "|3376|    3377|  3376.1|\n"
            + "|3377|    3378|  3377.1|\n"
            + "|3378|    3379|  3378.1|\n"
            + "|3379|    3380|  3379.1|\n"
            + "+----+--------+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(queryOverDeleteRange, expected);
  }

  @Test
  public void testMultiRangeDelete() {
    if (!isAbleToDelete) {
      return;
    }
    String delete =
        "DELETE FROM us.d1.s1 WHERE key > 105 AND key < 115 OR key >= 120 AND key <= 230;";
    executor.execute(delete);

    String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE key > 100 AND key < 235;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s1|\n"
            + "+---+--------+\n"
            + "|101|     101|\n"
            + "|102|     102|\n"
            + "|103|     103|\n"
            + "|104|     104|\n"
            + "|105|     105|\n"
            + "|115|     115|\n"
            + "|116|     116|\n"
            + "|117|     117|\n"
            + "|118|     118|\n"
            + "|119|     119|\n"
            + "|231|     231|\n"
            + "|232|     232|\n"
            + "|233|     233|\n"
            + "|234|     234|\n"
            + "+---+--------+\n"
            + "Total line number = 14\n";
    executor.executeAndCompare(queryOverDeleteRange, expected);

    delete =
        "DELETE FROM us.d1.s2, us.d1.s4 WHERE key > 1115 AND key <= 1125 OR key >= 1130 AND key < 1230;";
    executor.execute(delete);

    queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE key > 1110 AND key < 1235;";
    expected =
        "ResultSets:\n"
            + "+----+--------+--------+\n"
            + "| key|us.d1.s2|us.d1.s4|\n"
            + "+----+--------+--------+\n"
            + "|1111|    1112|  1111.1|\n"
            + "|1112|    1113|  1112.1|\n"
            + "|1113|    1114|  1113.1|\n"
            + "|1114|    1115|  1114.1|\n"
            + "|1115|    1116|  1115.1|\n"
            + "|1126|    1127|  1126.1|\n"
            + "|1127|    1128|  1127.1|\n"
            + "|1128|    1129|  1128.1|\n"
            + "|1129|    1130|  1129.1|\n"
            + "|1230|    1231|  1230.1|\n"
            + "|1231|    1232|  1231.1|\n"
            + "|1232|    1233|  1232.1|\n"
            + "|1233|    1234|  1233.1|\n"
            + "|1234|    1235|  1234.1|\n"
            + "+----+--------+--------+\n"
            + "Total line number = 14\n";
    executor.executeAndCompare(queryOverDeleteRange, expected);
  }

  @Test
  public void testCrossRangeDelete() {
    if (!isAbleToDelete) {
      return;
    }
    String delete =
        "DELETE FROM us.d1.s1 WHERE key > 205 AND key < 215 OR key >= 210 AND key <= 230;";
    executor.execute(delete);

    String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE key > 200 AND key < 235;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|us.d1.s1|\n"
            + "+---+--------+\n"
            + "|201|     201|\n"
            + "|202|     202|\n"
            + "|203|     203|\n"
            + "|204|     204|\n"
            + "|205|     205|\n"
            + "|231|     231|\n"
            + "|232|     232|\n"
            + "|233|     233|\n"
            + "|234|     234|\n"
            + "+---+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(queryOverDeleteRange, expected);

    delete =
        "DELETE FROM us.d1.s2, us.d1.s4 WHERE key > 1115 AND key <= 1125 OR key >= 1120 AND key < 1230;";
    executor.execute(delete);

    queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE key > 1110 AND key < 1235;";
    expected =
        "ResultSets:\n"
            + "+----+--------+--------+\n"
            + "| key|us.d1.s2|us.d1.s4|\n"
            + "+----+--------+--------+\n"
            + "|1111|    1112|  1111.1|\n"
            + "|1112|    1113|  1112.1|\n"
            + "|1113|    1114|  1113.1|\n"
            + "|1114|    1115|  1114.1|\n"
            + "|1115|    1116|  1115.1|\n"
            + "|1230|    1231|  1230.1|\n"
            + "|1231|    1232|  1231.1|\n"
            + "|1232|    1233|  1232.1|\n"
            + "|1233|    1234|  1233.1|\n"
            + "|1234|    1235|  1234.1|\n"
            + "+----+--------+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(queryOverDeleteRange, expected);
  }

  @Test
  public void testGroupBy() {
    String insert =
        "insert into test(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);

    String query = "select * from test;";
    String expected =
        "ResultSets:\n"
            + "+---+------+------+------+------+\n"
            + "|key|test.a|test.b|test.c|test.d|\n"
            + "+---+------+------+------+------+\n"
            + "|  1|     3|     2|   3.1|  val1|\n"
            + "|  2|     1|     3|   2.1|  val2|\n"
            + "|  3|     2|     2|   1.1|  val5|\n"
            + "|  4|     3|     2|   2.1|  val2|\n"
            + "|  5|     1|     2|   3.1|  val1|\n"
            + "|  6|     2|     2|   5.1|  val3|\n"
            + "+---+------+------+------+------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(query, expected);

    query = "select avg(a), b from test group by b order by b;";
    expected =
        "ResultSets:\n"
            + "+-----------+------+\n"
            + "|avg(test.a)|test.b|\n"
            + "+-----------+------+\n"
            + "|        2.2|     2|\n"
            + "|        1.0|     3|\n"
            + "+-----------+------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);

    query = "select avg(a), b, d from test group by b, d order by b, d;";
    expected =
        "ResultSets:\n"
            + "+-----------+------+------+\n"
            + "|avg(test.a)|test.b|test.d|\n"
            + "+-----------+------+------+\n"
            + "|        2.0|     2|  val1|\n"
            + "|        3.0|     2|  val2|\n"
            + "|        2.0|     2|  val3|\n"
            + "|        2.0|     2|  val5|\n"
            + "|        1.0|     3|  val2|\n"
            + "+-----------+------+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "select avg(a), c, b, d from test group by c, b, d order by c, b, d";
    expected =
        "ResultSets:\n"
            + "+-----------+------+------+------+\n"
            + "|avg(test.a)|test.c|test.b|test.d|\n"
            + "+-----------+------+------+------+\n"
            + "|        2.0|   1.1|     2|  val5|\n"
            + "|        3.0|   2.1|     2|  val2|\n"
            + "|        1.0|   2.1|     3|  val2|\n"
            + "|        2.0|   3.1|     2|  val1|\n"
            + "|        2.0|   5.1|     2|  val3|\n"
            + "+-----------+------+------+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "select avg(a), c, b, d from test group by c, b, d order by c, b, d";
    expected =
        "ResultSets:\n"
            + "+-----------+------+------+------+\n"
            + "|avg(test.a)|test.c|test.b|test.d|\n"
            + "+-----------+------+------+------+\n"
            + "|        2.0|   1.1|     2|  val5|\n"
            + "|        3.0|   2.1|     2|  val2|\n"
            + "|        1.0|   2.1|     3|  val2|\n"
            + "|        2.0|   3.1|     2|  val1|\n"
            + "|        2.0|   5.1|     2|  val3|\n"
            + "+-----------+------+------+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "select min(a), c from test group by c order by c;";
    expected =
        "ResultSets:\n"
            + "+-----------+------+\n"
            + "|min(test.a)|test.c|\n"
            + "+-----------+------+\n"
            + "|          2|   1.1|\n"
            + "|          1|   2.1|\n"
            + "|          1|   3.1|\n"
            + "|          2|   5.1|\n"
            + "+-----------+------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query = "select min(a), c from test group by c order by c;";
    expected =
        "ResultSets:\n"
            + "+-----------+------+\n"
            + "|min(test.a)|test.c|\n"
            + "+-----------+------+\n"
            + "|          2|   1.1|\n"
            + "|          1|   2.1|\n"
            + "|          1|   3.1|\n"
            + "|          2|   5.1|\n"
            + "+-----------+------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query = "select max(a), c from test group by c order by c;";
    expected =
        "ResultSets:\n"
            + "+-----------+------+\n"
            + "|max(test.a)|test.c|\n"
            + "+-----------+------+\n"
            + "|          2|   1.1|\n"
            + "|          3|   2.1|\n"
            + "|          3|   3.1|\n"
            + "|          2|   5.1|\n"
            + "+-----------+------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testGroupByAndHaving() {
    String insert =
        "insert into test(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);

    String query = "select * from test;";
    String expected =
        "ResultSets:\n"
            + "+---+------+------+------+------+\n"
            + "|key|test.a|test.b|test.c|test.d|\n"
            + "+---+------+------+------+------+\n"
            + "|  1|     3|     2|   3.1|  val1|\n"
            + "|  2|     1|     3|   2.1|  val2|\n"
            + "|  3|     2|     2|   1.1|  val5|\n"
            + "|  4|     3|     2|   2.1|  val2|\n"
            + "|  5|     1|     2|   3.1|  val1|\n"
            + "|  6|     2|     2|   5.1|  val3|\n"
            + "+---+------+------+------+------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(query, expected);

    query = "select avg(a), b from test group by b having avg(a) < 2;";
    expected =
        "ResultSets:\n"
            + "+-----------+------+\n"
            + "|avg(test.a)|test.b|\n"
            + "+-----------+------+\n"
            + "|        1.0|     3|\n"
            + "+-----------+------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);

    query = "select min(a), c from test group by c having c > 1.5 order by c;";
    expected =
        "ResultSets:\n"
            + "+-----------+------+\n"
            + "|min(test.a)|test.c|\n"
            + "+-----------+------+\n"
            + "|          1|   2.1|\n"
            + "|          1|   3.1|\n"
            + "|          2|   5.1|\n"
            + "+-----------+------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(query, expected);

    query = "select max(a), c from test group by c having max(a) > 2 order by c;";
    expected =
        "ResultSets:\n"
            + "+-----------+------+\n"
            + "|max(test.a)|test.c|\n"
            + "+-----------+------+\n"
            + "|          3|   2.1|\n"
            + "|          3|   3.1|\n"
            + "+-----------+------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testJoinWithGroupBy() {
    String insert =
        "insert into test1(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\")";
    executor.execute(insert);
    insert =
        "insert into test2(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\")";
    executor.execute(insert);

    String query = "select * from test1 join test2 on test1.a = test2.a";
    String expected =
        "ResultSets:\n"
            + "+-------+-------+-------+-------+---------+-------+-------+-------+-------+---------+\n"
            + "|test1.a|test1.b|test1.c|test1.d|test1.key|test2.a|test2.b|test2.c|test2.d|test2.key|\n"
            + "+-------+-------+-------+-------+---------+-------+-------+-------+-------+---------+\n"
            + "|      3|      2|    3.1|   val1|        1|      3|      2|    3.1|   val1|        1|\n"
            + "|      3|      2|    3.1|   val1|        1|      3|      2|    2.1|   val2|        4|\n"
            + "|      1|      3|    2.1|   val2|        2|      1|      3|    2.1|   val2|        2|\n"
            + "|      1|      3|    2.1|   val2|        2|      1|      2|    3.1|   val1|        5|\n"
            + "|      2|      2|    1.1|   val5|        3|      2|      2|    1.1|   val5|        3|\n"
            + "|      2|      2|    1.1|   val5|        3|      2|      2|    5.1|   val3|        6|\n"
            + "|      3|      2|    2.1|   val2|        4|      3|      2|    3.1|   val1|        1|\n"
            + "|      3|      2|    2.1|   val2|        4|      3|      2|    2.1|   val2|        4|\n"
            + "|      1|      2|    3.1|   val1|        5|      1|      3|    2.1|   val2|        2|\n"
            + "|      1|      2|    3.1|   val1|        5|      1|      2|    3.1|   val1|        5|\n"
            + "|      2|      2|    5.1|   val3|        6|      2|      2|    1.1|   val5|        3|\n"
            + "|      2|      2|    5.1|   val3|        6|      2|      2|    5.1|   val3|        6|\n"
            + "+-------+-------+-------+-------+---------+-------+-------+-------+-------+---------+\n"
            + "Total line number = 12\n";
    executor.executeAndCompare(query, expected);

    query =
        "select avg(test1.a), test2.d from test1 join test2 on test1.a = test2.a group by test2.d order by test2.d desc";
    expected =
        "ResultSets:\n"
            + "+------------+-------+\n"
            + "|avg(test1.a)|test2.d|\n"
            + "+------------+-------+\n"
            + "|         2.0|   val5|\n"
            + "|         2.0|   val3|\n"
            + "|         2.0|   val2|\n"
            + "|         2.0|   val1|\n"
            + "+------------+-------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query =
        "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d order by test2.d desc";
    expected =
        "ResultSets:\n"
            + "+------------+------------+-------+\n"
            + "|avg(test1.a)|max(test1.c)|test2.d|\n"
            + "+------------+------------+-------+\n"
            + "|         2.0|         5.1|   val5|\n"
            + "|         2.0|         5.1|   val3|\n"
            + "|         2.0|         3.1|   val2|\n"
            + "|         2.0|         3.1|   val1|\n"
            + "+------------+------------+-------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query =
        "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d having max(test1.c) > 3.5 order by test2.d desc";
    expected =
        "ResultSets:\n"
            + "+------------+------------+-------+\n"
            + "|avg(test1.a)|max(test1.c)|test2.d|\n"
            + "+------------+------------+-------+\n"
            + "|         2.0|         5.1|   val5|\n"
            + "|         2.0|         5.1|   val3|\n"
            + "+------------+------------+-------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);

    query =
        "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d having max(test1.c) > 3.5 order by test2.d";
    expected =
        "ResultSets:\n"
            + "+------------+------------+-------+\n"
            + "|avg(test1.a)|max(test1.c)|test2.d|\n"
            + "+------------+------------+-------+\n"
            + "|         2.0|         5.1|   val3|\n"
            + "|         2.0|         5.1|   val5|\n"
            + "+------------+------------+-------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);

    query =
        "select avg_a, test2.d as res from (select avg(test1.a) as avg_a, max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d having max(test1.c) > 3.5 order by test2.d limit 1);";
    expected =
        "ResultSets:\n"
            + "+-----+----+\n"
            + "|avg_a| res|\n"
            + "+-----+----+\n"
            + "|  2.0|val3|\n"
            + "+-----+----+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testJoin() {
    String insert =
        "insert into test(key, a.a, a.b, b.a, b.b) values (1, 1, 1.1, 2, 2.1), (2, 3, 3.1, 3, 3.1), (3, 5, 5.1, 4, 4.1), (4, 7, 7.1, 5, 5.1), (5, 9, 9.1, 6, 6.1);";
    executor.execute(insert);

    String statement = "select * from test.a join test.b on test.a.a = test.b.a";
    String expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     3.1|         2|\n"
            + "|       5|     5.1|         3|       5|     5.1|         4|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a inner join test.b on test.a.a = test.b.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     3.1|         2|\n"
            + "|       5|     5.1|         3|       5|     5.1|         4|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a left join test.b on test.a.a = test.b.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     3.1|         2|\n"
            + "|       5|     5.1|         3|       5|     5.1|         4|\n"
            + "|       1|     1.1|         1|    null|    null|      null|\n"
            + "|       7|     7.1|         4|    null|    null|      null|\n"
            + "|       9|     9.1|         5|    null|    null|      null|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a left join test.b using a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "|       3|     3.1|         2|     3.1|         2|\n"
            + "|       5|     5.1|         3|     5.1|         4|\n"
            + "|       1|     1.1|         1|    null|      null|\n"
            + "|       7|     7.1|         4|    null|      null|\n"
            + "|       9|     9.1|         5|    null|      null|\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a right join test.b on test.a.a = test.b.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     3.1|         2|\n"
            + "|       5|     5.1|         3|       5|     5.1|         4|\n"
            + "|    null|    null|      null|       2|     2.1|         1|\n"
            + "|    null|    null|      null|       4|     4.1|         3|\n"
            + "|    null|    null|      null|       6|     6.1|         5|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a right join test.b using a";
    expected =
        "ResultSets:\n"
            + "+--------+----------+--------+--------+----------+\n"
            + "|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+----------+--------+--------+----------+\n"
            + "|     3.1|         2|       3|     3.1|         2|\n"
            + "|     5.1|         3|       5|     5.1|         4|\n"
            + "|    null|      null|       2|     2.1|         1|\n"
            + "|    null|      null|       4|     4.1|         3|\n"
            + "|    null|      null|       6|     6.1|         5|\n"
            + "+--------+----------+--------+--------+----------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a full join test.b on test.a.a = test.b.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     3.1|         2|\n"
            + "|       5|     5.1|         3|       5|     5.1|         4|\n"
            + "|       1|     1.1|         1|    null|    null|      null|\n"
            + "|       7|     7.1|         4|    null|    null|      null|\n"
            + "|       9|     9.1|         5|    null|    null|      null|\n"
            + "|    null|    null|      null|       2|     2.1|         1|\n"
            + "|    null|    null|      null|       4|     4.1|         3|\n"
            + "|    null|    null|      null|       6|     6.1|         5|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a, test.b";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       1|     1.1|         1|       2|     2.1|         1|\n"
            + "|       1|     1.1|         1|       3|     3.1|         2|\n"
            + "|       1|     1.1|         1|       4|     4.1|         3|\n"
            + "|       1|     1.1|         1|       5|     5.1|         4|\n"
            + "|       1|     1.1|         1|       6|     6.1|         5|\n"
            + "|       3|     3.1|         2|       2|     2.1|         1|\n"
            + "|       3|     3.1|         2|       3|     3.1|         2|\n"
            + "|       3|     3.1|         2|       4|     4.1|         3|\n"
            + "|       3|     3.1|         2|       5|     5.1|         4|\n"
            + "|       3|     3.1|         2|       6|     6.1|         5|\n"
            + "|       5|     5.1|         3|       2|     2.1|         1|\n"
            + "|       5|     5.1|         3|       3|     3.1|         2|\n"
            + "|       5|     5.1|         3|       4|     4.1|         3|\n"
            + "|       5|     5.1|         3|       5|     5.1|         4|\n"
            + "|       5|     5.1|         3|       6|     6.1|         5|\n"
            + "|       7|     7.1|         4|       2|     2.1|         1|\n"
            + "|       7|     7.1|         4|       3|     3.1|         2|\n"
            + "|       7|     7.1|         4|       4|     4.1|         3|\n"
            + "|       7|     7.1|         4|       5|     5.1|         4|\n"
            + "|       7|     7.1|         4|       6|     6.1|         5|\n"
            + "|       9|     9.1|         5|       2|     2.1|         1|\n"
            + "|       9|     9.1|         5|       3|     3.1|         2|\n"
            + "|       9|     9.1|         5|       4|     4.1|         3|\n"
            + "|       9|     9.1|         5|       5|     5.1|         4|\n"
            + "|       9|     9.1|         5|       6|     6.1|         5|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 25\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a, test.b where test.a.a = test.b.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     3.1|         2|\n"
            + "|       5|     5.1|         3|       5|     5.1|         4|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a cross join test.b";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       1|     1.1|         1|       2|     2.1|         1|\n"
            + "|       1|     1.1|         1|       3|     3.1|         2|\n"
            + "|       1|     1.1|         1|       4|     4.1|         3|\n"
            + "|       1|     1.1|         1|       5|     5.1|         4|\n"
            + "|       1|     1.1|         1|       6|     6.1|         5|\n"
            + "|       3|     3.1|         2|       2|     2.1|         1|\n"
            + "|       3|     3.1|         2|       3|     3.1|         2|\n"
            + "|       3|     3.1|         2|       4|     4.1|         3|\n"
            + "|       3|     3.1|         2|       5|     5.1|         4|\n"
            + "|       3|     3.1|         2|       6|     6.1|         5|\n"
            + "|       5|     5.1|         3|       2|     2.1|         1|\n"
            + "|       5|     5.1|         3|       3|     3.1|         2|\n"
            + "|       5|     5.1|         3|       4|     4.1|         3|\n"
            + "|       5|     5.1|         3|       5|     5.1|         4|\n"
            + "|       5|     5.1|         3|       6|     6.1|         5|\n"
            + "|       7|     7.1|         4|       2|     2.1|         1|\n"
            + "|       7|     7.1|         4|       3|     3.1|         2|\n"
            + "|       7|     7.1|         4|       4|     4.1|         3|\n"
            + "|       7|     7.1|         4|       5|     5.1|         4|\n"
            + "|       7|     7.1|         4|       6|     6.1|         5|\n"
            + "|       9|     9.1|         5|       2|     2.1|         1|\n"
            + "|       9|     9.1|         5|       3|     3.1|         2|\n"
            + "|       9|     9.1|         5|       4|     4.1|         3|\n"
            + "|       9|     9.1|         5|       5|     5.1|         4|\n"
            + "|       9|     9.1|         5|       6|     6.1|         5|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 25\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testMultiJoin() {
    String insert =
        "insert into test(key, a.a, a.b) values (1, 1, 1.1), (2, 3, 3.1), (3, 5, 5.1), (4, 7, 7.1), (5, 9, 9.1);";
    executor.execute(insert);

    insert =
        "insert into test(key, b.a, b.b) values (1, 2, \"aaa\"), (2, 3, \"bbb\"), (3, 4, \"ccc\"), (4, 5, \"ddd\"), (5, 6, \"eee\");";
    executor.execute(insert);

    insert =
        "insert into test(key, c.a, c.b) values (1, \"ddd\", true), (2, \"eee\", false), (3, \"aaa\", true), (4, \"bbb\", false), (5, \"ccc\", true);";
    executor.execute(insert);

    String statement = "select * from test";
    String expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.b.a|test.b.b|test.c.a|test.c.b|\n"
            + "+---+--------+--------+--------+--------+--------+--------+\n"
            + "|  1|       1|     1.1|       2|     aaa|     ddd|    true|\n"
            + "|  2|       3|     3.1|       3|     bbb|     eee|   false|\n"
            + "|  3|       5|     5.1|       4|     ccc|     aaa|    true|\n"
            + "|  4|       7|     7.1|       5|     ddd|     bbb|   false|\n"
            + "|  5|       9|     9.1|       6|     eee|     ccc|    true|\n"
            + "+---+--------+--------+--------+--------+--------+--------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "select * from test.a join test.b on test.a.a = test.b.a join test.c on test.b.b = test.c.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|test.c.a|test.c.b|test.c.key|\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     bbb|         2|     bbb|   false|         4|\n"
            + "|       5|     5.1|         3|       5|     ddd|         4|     ddd|    true|         1|\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "select * from test.a, test.b, test.c where test.a.a = test.b.a and test.b.b = test.c.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|test.c.a|test.c.b|test.c.key|\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     bbb|         2|     bbb|   false|         4|\n"
            + "|       5|     5.1|         3|       5|     ddd|         4|     ddd|    true|         1|\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "select * from test.a full join test.b on test.a.a = test.b.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     bbb|         2|\n"
            + "|       5|     5.1|         3|       5|     ddd|         4|\n"
            + "|       1|     1.1|         1|    null|    null|      null|\n"
            + "|       7|     7.1|         4|    null|    null|      null|\n"
            + "|       9|     9.1|         5|    null|    null|      null|\n"
            + "|    null|    null|      null|       2|     aaa|         1|\n"
            + "|    null|    null|      null|       4|     ccc|         3|\n"
            + "|    null|    null|      null|       6|     eee|         5|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "select * from test.a full join test.b on test.a.a = test.b.a full join test.c on test.b.b = test.c.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|test.c.a|test.c.b|test.c.key|\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       3|     bbb|         2|     bbb|   false|         4|\n"
            + "|       5|     5.1|         3|       5|     ddd|         4|     ddd|    true|         1|\n"
            + "|    null|    null|      null|       2|     aaa|         1|     aaa|    true|         3|\n"
            + "|    null|    null|      null|       4|     ccc|         3|     ccc|    true|         5|\n"
            + "|    null|    null|      null|       6|     eee|         5|     eee|   false|         2|\n"
            + "|       1|     1.1|         1|    null|    null|      null|    null|    null|      null|\n"
            + "|       7|     7.1|         4|    null|    null|      null|    null|    null|      null|\n"
            + "|       9|     9.1|         5|    null|    null|      null|    null|    null|      null|\n"
            + "+--------+--------+----------+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testBasicArithmeticExpr() {
    String insert =
        "INSERT INTO us.d3 (key, s1, s2, s3) values "
            + "(1, 1, 6, 1.5), (2, 2, 5, 2.5), (3, 3, 4, 3.5), (4, 4, 3, 4.5), (5, 5, 2, 5.5), (6, 6, 1, 6.5);";
    executor.execute(insert);

    String statement = "SELECT s1, s2, s3 FROM us.d3;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|us.d3.s1|us.d3.s2|us.d3.s3|\n"
            + "+---+--------+--------+--------+\n"
            + "|  1|       1|       6|     1.5|\n"
            + "|  2|       2|       5|     2.5|\n"
            + "|  3|       3|       4|     3.5|\n"
            + "|  4|       4|       3|     4.5|\n"
            + "|  5|       5|       2|     5.5|\n"
            + "|  6|       6|       1|     6.5|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT s1+1, s2-1, s3*2 FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+---+------------+------------+------------+\n"
            + "|key|us.d3.s1 + 1|us.d3.s2 - 1|us.d3.s3 × 2|\n"
            + "+---+------------+------------+------------+\n"
            + "|  1|           2|           5|         3.0|\n"
            + "|  2|           3|           4|         5.0|\n"
            + "|  3|           4|           3|         7.0|\n"
            + "|  4|           5|           2|         9.0|\n"
            + "|  5|           6|           1|        11.0|\n"
            + "|  6|           7|           0|        13.0|\n"
            + "+---+------------+------------+------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT s1+s2, s1-s2, s1+s3, s1-s3 FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+---+-------------------+-------------------+-------------------+-------------------+\n"
            + "|key|us.d3.s1 + us.d3.s2|us.d3.s1 - us.d3.s2|us.d3.s1 + us.d3.s3|us.d3.s1 - us.d3.s3|\n"
            + "+---+-------------------+-------------------+-------------------+-------------------+\n"
            + "|  1|                  7|                 -5|                2.5|               -0.5|\n"
            + "|  2|                  7|                 -3|                4.5|               -0.5|\n"
            + "|  3|                  7|                 -1|                6.5|               -0.5|\n"
            + "|  4|                  7|                  1|                8.5|               -0.5|\n"
            + "|  5|                  7|                  3|               10.5|               -0.5|\n"
            + "|  6|                  7|                  5|               12.5|               -0.5|\n"
            + "+---+-------------------+-------------------+-------------------+-------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT s1*s2, s1/s2, s1%s2 FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+---+-------------------+-------------------+-------------------+\n"
            + "|key|us.d3.s1 × us.d3.s2|us.d3.s1 ÷ us.d3.s2|us.d3.s1 % us.d3.s2|\n"
            + "+---+-------------------+-------------------+-------------------+\n"
            + "|  1|                  6|                  0|                  1|\n"
            + "|  2|                 10|                  0|                  2|\n"
            + "|  3|                 12|                  0|                  3|\n"
            + "|  4|                 12|                  1|                  1|\n"
            + "|  5|                 10|                  2|                  1|\n"
            + "|  6|                  6|                  6|                  0|\n"
            + "+---+-------------------+-------------------+-------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT s1*s3, s1/s3, s1%s3 FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+---+-------------------+-------------------+-------------------+\n"
            + "|key|us.d3.s1 × us.d3.s3|us.d3.s1 ÷ us.d3.s3|us.d3.s1 % us.d3.s3|\n"
            + "+---+-------------------+-------------------+-------------------+\n"
            + "|  1|                1.5| 0.6666666666666666|                1.0|\n"
            + "|  2|                5.0|                0.8|                2.0|\n"
            + "|  3|               10.5| 0.8571428571428571|                3.0|\n"
            + "|  4|               18.0| 0.8888888888888888|                4.0|\n"
            + "|  5|               27.5| 0.9090909090909091|                5.0|\n"
            + "|  6|               39.0| 0.9230769230769231|                6.0|\n"
            + "+---+-------------------+-------------------+-------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testComplexArithmeticExpr() {
    String insert =
        "INSERT INTO us.d3 (key, s1, s2, s3) values "
            + "(1, 1, 6, 1.5), (2, 2, 5, 2.5), (3, 3, 4, 3.5), (4, 4, 3, 4.5), (5, 5, 2, 5.5), (6, 6, 1, 6.5);";
    executor.execute(insert);

    String statement = "SELECT s1, s2, s3 FROM us.d3;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|us.d3.s1|us.d3.s2|us.d3.s3|\n"
            + "+---+--------+--------+--------+\n"
            + "|  1|       1|       6|     1.5|\n"
            + "|  2|       2|       5|     2.5|\n"
            + "|  3|       3|       4|     3.5|\n"
            + "|  4|       4|       3|     4.5|\n"
            + "|  5|       5|       2|     5.5|\n"
            + "|  6|       6|       1|     6.5|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT (s1+s2)*s3 FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+---+--------------------------------+\n"
            + "|key|(us.d3.s1 + us.d3.s2) × us.d3.s3|\n"
            + "+---+--------------------------------+\n"
            + "|  1|                            10.5|\n"
            + "|  2|                            17.5|\n"
            + "|  3|                            24.5|\n"
            + "|  4|                            31.5|\n"
            + "|  5|                            38.5|\n"
            + "|  6|                            45.5|\n"
            + "+---+--------------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT (s1+s3)*(s2-s3) FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+---+---------------------------------------------+\n"
            + "|key|(us.d3.s1 + us.d3.s3) × (us.d3.s2 - us.d3.s3)|\n"
            + "+---+---------------------------------------------+\n"
            + "|  1|                                        11.25|\n"
            + "|  2|                                        11.25|\n"
            + "|  3|                                         3.25|\n"
            + "|  4|                                       -12.75|\n"
            + "|  5|                                       -36.75|\n"
            + "|  6|                                       -68.75|\n"
            + "+---+---------------------------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT ((s1+s2)*s3+s2)*s3 FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+---+--------------------------------------------------------+\n"
            + "|key|((us.d3.s1 + us.d3.s2) × us.d3.s3 + us.d3.s2) × us.d3.s3|\n"
            + "+---+--------------------------------------------------------+\n"
            + "|  1|                                                   24.75|\n"
            + "|  2|                                                   56.25|\n"
            + "|  3|                                                   99.75|\n"
            + "|  4|                                                  155.25|\n"
            + "|  5|                                                  222.75|\n"
            + "|  6|                                                  302.25|\n"
            + "+---+--------------------------------------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT (s1+1)*(s2-1)*(s3*2) FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+---+------------------------------------------------+\n"
            + "|key|(us.d3.s1 + 1) × (us.d3.s2 - 1) × (us.d3.s3 × 2)|\n"
            + "+---+------------------------------------------------+\n"
            + "|  1|                                            30.0|\n"
            + "|  2|                                            60.0|\n"
            + "|  3|                                            84.0|\n"
            + "|  4|                                            90.0|\n"
            + "|  5|                                            66.0|\n"
            + "|  6|                                             0.0|\n"
            + "+---+------------------------------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testAlias() {
    // time series alias
    String statement = "SELECT s1 AS rename_series, s2 FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010;";
    String expected =
        "ResultSets:\n"
            + "+----+-------------+--------+\n"
            + "| key|rename_series|us.d1.s2|\n"
            + "+----+-------------+--------+\n"
            + "|1000|         1000|    1001|\n"
            + "|1001|         1001|    1002|\n"
            + "|1002|         1002|    1003|\n"
            + "|1003|         1003|    1004|\n"
            + "|1004|         1004|    1005|\n"
            + "|1005|         1005|    1006|\n"
            + "|1006|         1006|    1007|\n"
            + "|1007|         1007|    1008|\n"
            + "|1008|         1008|    1009|\n"
            + "|1009|         1009|    1010|\n"
            + "+----+-------------+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    // time series alias with arithmetic expression
    statement =
        "SELECT s1+s2 AS rename_sum, s2-s1 AS rename_diff FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010;";
    expected =
        "ResultSets:\n"
            + "+----+----------+-----------+\n"
            + "| key|rename_sum|rename_diff|\n"
            + "+----+----------+-----------+\n"
            + "|1000|      2001|          1|\n"
            + "|1001|      2003|          1|\n"
            + "|1002|      2005|          1|\n"
            + "|1003|      2007|          1|\n"
            + "|1004|      2009|          1|\n"
            + "|1005|      2011|          1|\n"
            + "|1006|      2013|          1|\n"
            + "|1007|      2015|          1|\n"
            + "|1008|      2017|          1|\n"
            + "|1009|      2019|          1|\n"
            + "+----+----------+-----------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    // path alias
    statement = "SELECT s1, s2 FROM us.d1 AS rename_path WHERE s1 >= 1000 AND s1 < 1010;";
    expected =
        "ResultSets:\n"
            + "+----+--------------+--------------+\n"
            + "| key|rename_path.s1|rename_path.s2|\n"
            + "+----+--------------+--------------+\n"
            + "|1000|          1000|          1001|\n"
            + "|1001|          1001|          1002|\n"
            + "|1002|          1002|          1003|\n"
            + "|1003|          1003|          1004|\n"
            + "|1004|          1004|          1005|\n"
            + "|1005|          1005|          1006|\n"
            + "|1006|          1006|          1007|\n"
            + "|1007|          1007|          1008|\n"
            + "|1008|          1008|          1009|\n"
            + "|1009|          1009|          1010|\n"
            + "+----+--------------+--------------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    // sub-query alias
    statement =
        "SELECT * FROM (SELECT s1, s2 FROM us.d1 WHERE us.d1.s1 >= 1000 AND us.d1.s1 < 1010) AS rename_result_set;";
    expected =
        "ResultSets:\n"
            + "+----+--------------------------+--------------------------+\n"
            + "| key|rename_result_set.us.d1.s1|rename_result_set.us.d1.s2|\n"
            + "+----+--------------------------+--------------------------+\n"
            + "|1000|                      1000|                      1001|\n"
            + "|1001|                      1001|                      1002|\n"
            + "|1002|                      1002|                      1003|\n"
            + "|1003|                      1003|                      1004|\n"
            + "|1004|                      1004|                      1005|\n"
            + "|1005|                      1005|                      1006|\n"
            + "|1006|                      1006|                      1007|\n"
            + "|1007|                      1007|                      1008|\n"
            + "|1008|                      1008|                      1009|\n"
            + "|1009|                      1009|                      1010|\n"
            + "+----+--------------------------+--------------------------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    // sub-query alias with arithmetic expression
    statement =
        "SELECT * FROM (SELECT s1+s2 FROM us.d1 WHERE us.d1.s1 >= 1000 AND us.d1.s1 < 1010) AS rename_result_set;";
    expected =
        "ResultSets:\n"
            + "+----+---------------------------------------+\n"
            + "| key|rename_result_set.(us.d1.s1 + us.d1.s2)|\n"
            + "+----+---------------------------------------+\n"
            + "|1000|                                   2001|\n"
            + "|1001|                                   2003|\n"
            + "|1002|                                   2005|\n"
            + "|1003|                                   2007|\n"
            + "|1004|                                   2009|\n"
            + "|1005|                                   2011|\n"
            + "|1006|                                   2013|\n"
            + "|1007|                                   2015|\n"
            + "|1008|                                   2017|\n"
            + "|1009|                                   2019|\n"
            + "+----+---------------------------------------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    // time series and sub-query alias
    statement =
        "SELECT * FROM (SELECT s1 AS rename_series, s2 FROM us.d1 WHERE us.d1.s1 >= 1000 AND us.d1.s1 < 1010) AS rename_result_set;";
    expected =
        "ResultSets:\n"
            + "+----+-------------------------------+--------------------------+\n"
            + "| key|rename_result_set.rename_series|rename_result_set.us.d1.s2|\n"
            + "+----+-------------------------------+--------------------------+\n"
            + "|1000|                           1000|                      1001|\n"
            + "|1001|                           1001|                      1002|\n"
            + "|1002|                           1002|                      1003|\n"
            + "|1003|                           1003|                      1004|\n"
            + "|1004|                           1004|                      1005|\n"
            + "|1005|                           1005|                      1006|\n"
            + "|1006|                           1006|                      1007|\n"
            + "|1007|                           1007|                      1008|\n"
            + "|1008|                           1008|                      1009|\n"
            + "|1009|                           1009|                      1010|\n"
            + "+----+-------------------------------+--------------------------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testAggregateSubQuery() {
    String statement =
        "SELECT %s_s1 FROM (SELECT %s(s1) AS %s_s1 FROM us.d1 OVER(RANGE 60 IN [1000, 1600)));";
    List<String> funcTypeList =
        Arrays.asList("max", "min", "sum", "avg", "count", "first_value", "last_value");

    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+----+------+\n"
                + "| key|max_s1|\n"
                + "+----+------+\n"
                + "|1000|  1059|\n"
                + "|1060|  1119|\n"
                + "|1120|  1179|\n"
                + "|1180|  1239|\n"
                + "|1240|  1299|\n"
                + "|1300|  1359|\n"
                + "|1360|  1419|\n"
                + "|1420|  1479|\n"
                + "|1480|  1539|\n"
                + "|1540|  1599|\n"
                + "+----+------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+------+\n"
                + "| key|min_s1|\n"
                + "+----+------+\n"
                + "|1000|  1000|\n"
                + "|1060|  1060|\n"
                + "|1120|  1120|\n"
                + "|1180|  1180|\n"
                + "|1240|  1240|\n"
                + "|1300|  1300|\n"
                + "|1360|  1360|\n"
                + "|1420|  1420|\n"
                + "|1480|  1480|\n"
                + "|1540|  1540|\n"
                + "+----+------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+------+\n"
                + "| key|sum_s1|\n"
                + "+----+------+\n"
                + "|1000| 61770|\n"
                + "|1060| 65370|\n"
                + "|1120| 68970|\n"
                + "|1180| 72570|\n"
                + "|1240| 76170|\n"
                + "|1300| 79770|\n"
                + "|1360| 83370|\n"
                + "|1420| 86970|\n"
                + "|1480| 90570|\n"
                + "|1540| 94170|\n"
                + "+----+------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+------+\n"
                + "| key|avg_s1|\n"
                + "+----+------+\n"
                + "|1000|1029.5|\n"
                + "|1060|1089.5|\n"
                + "|1120|1149.5|\n"
                + "|1180|1209.5|\n"
                + "|1240|1269.5|\n"
                + "|1300|1329.5|\n"
                + "|1360|1389.5|\n"
                + "|1420|1449.5|\n"
                + "|1480|1509.5|\n"
                + "|1540|1569.5|\n"
                + "+----+------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+--------+\n"
                + "| key|count_s1|\n"
                + "+----+--------+\n"
                + "|1000|      60|\n"
                + "|1060|      60|\n"
                + "|1120|      60|\n"
                + "|1180|      60|\n"
                + "|1240|      60|\n"
                + "|1300|      60|\n"
                + "|1360|      60|\n"
                + "|1420|      60|\n"
                + "|1480|      60|\n"
                + "|1540|      60|\n"
                + "+----+--------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+--------------+\n"
                + "| key|first_value_s1|\n"
                + "+----+--------------+\n"
                + "|1000|          1000|\n"
                + "|1060|          1060|\n"
                + "|1120|          1120|\n"
                + "|1180|          1180|\n"
                + "|1240|          1240|\n"
                + "|1300|          1300|\n"
                + "|1360|          1360|\n"
                + "|1420|          1420|\n"
                + "|1480|          1480|\n"
                + "|1540|          1540|\n"
                + "+----+--------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+-------------+\n"
                + "| key|last_value_s1|\n"
                + "+----+-------------+\n"
                + "|1000|         1059|\n"
                + "|1060|         1119|\n"
                + "|1120|         1179|\n"
                + "|1180|         1239|\n"
                + "|1240|         1299|\n"
                + "|1300|         1359|\n"
                + "|1360|         1419|\n"
                + "|1420|         1479|\n"
                + "|1480|         1539|\n"
                + "|1540|         1599|\n"
                + "+----+-------------+\n"
                + "Total line number = 10\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type, type), expected);
    }
  }

  @Test
  public void testValueFilterSubQuery() {
    String statement =
        "SELECT ts FROM (SELECT s1 AS ts FROM us.d1 WHERE us.d1.s1 >= 1000 AND us.d1.s1 < 1010);";
    String expected =
        "ResultSets:\n"
            + "+----+----+\n"
            + "| key|  ts|\n"
            + "+----+----+\n"
            + "|1000|1000|\n"
            + "|1001|1001|\n"
            + "|1002|1002|\n"
            + "|1003|1003|\n"
            + "|1004|1004|\n"
            + "|1005|1005|\n"
            + "|1006|1006|\n"
            + "|1007|1007|\n"
            + "|1008|1008|\n"
            + "|1009|1009|\n"
            + "+----+----+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT avg_s1 FROM (SELECT AVG(s1) AS avg_s1 FROM us.d1 OVER (RANGE 100 IN [1000, 1600))) WHERE avg_s1 > 1200;";
    expected =
        "ResultSets:\n"
            + "+----+------+\n"
            + "| key|avg_s1|\n"
            + "+----+------+\n"
            + "|1200|1249.5|\n"
            + "|1300|1349.5|\n"
            + "|1400|1449.5|\n"
            + "|1500|1549.5|\n"
            + "+----+------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT avg_s1 FROM (SELECT AVG(s1) AS avg_s1 FROM us.d1 WHERE us.d1.s1 < 1500 OVER (RANGE 100 IN [1000, 1600))) WHERE avg_s1 > 1200;";
    expected =
        "ResultSets:\n"
            + "+----+------+\n"
            + "| key|avg_s1|\n"
            + "+----+------+\n"
            + "|1200|1249.5|\n"
            + "|1300|1349.5|\n"
            + "|1400|1449.5|\n"
            + "+----+------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testMultiSubQuery() {
    String statement =
        "SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 OVER (RANGE 10 IN [1000, 1100));";
    String expected =
        "ResultSets:\n"
            + "+----+------+------+\n"
            + "| key|avg_s1|sum_s2|\n"
            + "+----+------+------+\n"
            + "|1000|1004.5| 10055|\n"
            + "|1010|1014.5| 10155|\n"
            + "|1020|1024.5| 10255|\n"
            + "|1030|1034.5| 10355|\n"
            + "|1040|1044.5| 10455|\n"
            + "|1050|1054.5| 10555|\n"
            + "|1060|1064.5| 10655|\n"
            + "|1070|1074.5| 10755|\n"
            + "|1080|1084.5| 10855|\n"
            + "|1090|1094.5| 10955|\n"
            + "+----+------+------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT avg_s1, sum_s2 "
            + "FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 "
            + "FROM us.d1 OVER (RANGE 10 IN [1000, 1100))) "
            + "WHERE avg_s1 > 1020 AND sum_s2 < 10800;";
    expected =
        "ResultSets:\n"
            + "+----+------+------+\n"
            + "| key|avg_s1|sum_s2|\n"
            + "+----+------+------+\n"
            + "|1020|1024.5| 10255|\n"
            + "|1030|1034.5| 10355|\n"
            + "|1040|1044.5| 10455|\n"
            + "|1050|1054.5| 10555|\n"
            + "|1060|1064.5| 10655|\n"
            + "|1070|1074.5| 10755|\n"
            + "+----+------+------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT MAX(avg_s1), MIN(sum_s2) "
            + "FROM (SELECT avg_s1, sum_s2 "
            + "FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 "
            + "FROM us.d1 OVER (RANGE 10 IN [1000, 1100))) "
            + "WHERE avg_s1 > 1020 AND sum_s2 < 10800);";
    expected =
        "ResultSets:\n"
            + "+-----------+-----------+\n"
            + "|max(avg_s1)|min(sum_s2)|\n"
            + "+-----------+-----------+\n"
            + "|     1074.5|      10255|\n"
            + "+-----------+-----------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testFromSubQuery() {
    String insert = "INSERT INTO test(key, a.a, a.b) VALUES (1, 1, 1.1), (2, 3, 3.1), (3, 7, 7.1);";
    executor.execute(insert);
    insert =
        "INSERT INTO test(key, b.a, b.b) VALUES (1, 2, \"aaa\"), (3, 4, \"ccc\"), (5, 6, \"eee\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test(key, c.a, c.b) VALUES (2, \"eee\", 1), (3, \"aaa\", 2), (4, \"bbb\", 3);";
    executor.execute(insert);

    String statement = "SELECT * FROM test.a, (SELECT * FROM test.b);";
    String expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       1|     1.1|         1|       2|     aaa|         1|\n"
            + "|       1|     1.1|         1|       4|     ccc|         3|\n"
            + "|       1|     1.1|         1|       6|     eee|         5|\n"
            + "|       3|     3.1|         2|       2|     aaa|         1|\n"
            + "|       3|     3.1|         2|       4|     ccc|         3|\n"
            + "|       3|     3.1|         2|       6|     eee|         5|\n"
            + "|       7|     7.1|         3|       2|     aaa|         1|\n"
            + "|       7|     7.1|         3|       4|     ccc|         3|\n"
            + "|       7|     7.1|         3|       6|     eee|         5|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(statement, expected);

    // self join
    statement = "SELECT * FROM test.a AS sub1, test.a AS sub2;";
    expected =
        "ResultSets:\n"
            + "+------+------+--------+------+------+--------+\n"
            + "|sub1.a|sub1.b|sub1.key|sub2.a|sub2.b|sub2.key|\n"
            + "+------+------+--------+------+------+--------+\n"
            + "|     1|   1.1|       1|     1|   1.1|       1|\n"
            + "|     1|   1.1|       1|     3|   3.1|       2|\n"
            + "|     1|   1.1|       1|     7|   7.1|       3|\n"
            + "|     3|   3.1|       2|     1|   1.1|       1|\n"
            + "|     3|   3.1|       2|     3|   3.1|       2|\n"
            + "|     3|   3.1|       2|     7|   7.1|       3|\n"
            + "|     7|   7.1|       3|     1|   1.1|       1|\n"
            + "|     7|   7.1|       3|     3|   3.1|       2|\n"
            + "|     7|   7.1|       3|     7|   7.1|       3|\n"
            + "+------+------+--------+------+------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM (SELECT * FROM test.a) AS sub1, (SELECT * FROM test.a) AS sub2;";
    expected =
        "ResultSets:\n"
            + "+--------+-------------+-------------+--------+-------------+-------------+\n"
            + "|sub1.key|sub1.test.a.a|sub1.test.a.b|sub2.key|sub2.test.a.a|sub2.test.a.b|\n"
            + "+--------+-------------+-------------+--------+-------------+-------------+\n"
            + "|       1|            1|          1.1|       1|            1|          1.1|\n"
            + "|       1|            1|          1.1|       2|            3|          3.1|\n"
            + "|       1|            1|          1.1|       3|            7|          7.1|\n"
            + "|       2|            3|          3.1|       1|            1|          1.1|\n"
            + "|       2|            3|          3.1|       2|            3|          3.1|\n"
            + "|       2|            3|          3.1|       3|            7|          7.1|\n"
            + "|       3|            7|          7.1|       1|            1|          1.1|\n"
            + "|       3|            7|          7.1|       2|            3|          3.1|\n"
            + "|       3|            7|          7.1|       3|            7|          7.1|\n"
            + "+--------+-------------+-------------+--------+-------------+-------------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM (SELECT * FROM test.b), test.a;";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       1|     1.1|         1|       2|     aaa|         1|\n"
            + "|       3|     3.1|         2|       2|     aaa|         1|\n"
            + "|       7|     7.1|         3|       2|     aaa|         1|\n"
            + "|       1|     1.1|         1|       4|     ccc|         3|\n"
            + "|       3|     3.1|         2|       4|     ccc|         3|\n"
            + "|       7|     7.1|         3|       4|     ccc|         3|\n"
            + "|       1|     1.1|         1|       6|     eee|         5|\n"
            + "|       3|     3.1|         2|       6|     eee|         5|\n"
            + "|       7|     7.1|         3|       6|     eee|         5|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a, (SELECT * FROM test.b WHERE test.b.a < 6) WHERE test.a.a > 1;";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       2|     aaa|         1|\n"
            + "|       3|     3.1|         2|       4|     ccc|         3|\n"
            + "|       7|     7.1|         3|       2|     aaa|         1|\n"
            + "|       7|     7.1|         3|       4|     ccc|         3|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a, (SELECT * FROM test.b WHERE test.b.a < test.a.a) WHERE test.a.a > 1;";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       2|     aaa|         1|\n"
            + "|       7|     7.1|         3|       2|     aaa|         1|\n"
            + "|       7|     7.1|         3|       4|     ccc|         3|\n"
            + "|       7|     7.1|         3|       6|     eee|         5|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a, (SELECT * FROM test.b WHERE test.b.a < test.a.a AND test.b.a < 6) WHERE test.a.a > 1;";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.b|test.b.key|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "|       3|     3.1|         2|       2|     aaa|         1|\n"
            + "|       7|     7.1|         3|       2|     aaa|         1|\n"
            + "|       7|     7.1|         3|       4|     ccc|         3|\n"
            + "+--------+--------+----------+--------+--------+----------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a, (SELECT a FROM test.b WHERE test.b.a < 6), (SELECT b FROM test.c WHERE test.c.b = 2);";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+----------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.key|test.c.b|test.c.key|\n"
            + "+--------+--------+----------+--------+----------+--------+----------+\n"
            + "|       1|     1.1|         1|       2|         1|       2|         3|\n"
            + "|       1|     1.1|         1|       4|         3|       2|         3|\n"
            + "|       3|     3.1|         2|       2|         1|       2|         3|\n"
            + "|       3|     3.1|         2|       4|         3|       2|         3|\n"
            + "|       7|     7.1|         3|       2|         1|       2|         3|\n"
            + "|       7|     7.1|         3|       4|         3|       2|         3|\n"
            + "+--------+--------+----------+--------+----------+--------+----------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a, (SELECT * FROM (SELECT a FROM test.b WHERE test.b.a < 6 AND test.b.a < test.a.a), (SELECT b FROM test.c WHERE test.c.b = 1)) AS sub;";
    expected =
        "ResultSets:\n"
            + "+------------+--------------+------------+--------------+--------+--------+----------+\n"
            + "|sub.test.b.a|sub.test.b.key|sub.test.c.b|sub.test.c.key|test.a.a|test.a.b|test.a.key|\n"
            + "+------------+--------------+------------+--------------+--------+--------+----------+\n"
            + "|           2|             1|           1|             2|       3|     3.1|         2|\n"
            + "|           2|             1|           1|             2|       7|     7.1|         3|\n"
            + "|           4|             3|           1|             2|       7|     7.1|         3|\n"
            + "+------------+--------------+------------+--------------+--------+--------+----------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a, (SELECT * FROM (SELECT a FROM test.b WHERE test.b.a < 6 AND test.b.a < test.a.a), (SELECT b FROM test.c WHERE test.c.b < test.a.a));";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+----------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.key|test.c.b|test.c.key|\n"
            + "+--------+--------+----------+--------+----------+--------+----------+\n"
            + "|       3|     3.1|         2|       2|         1|       1|         2|\n"
            + "|       3|     3.1|         2|       2|         1|       2|         3|\n"
            + "|       7|     7.1|         3|       2|         1|       1|         2|\n"
            + "|       7|     7.1|         3|       2|         1|       2|         3|\n"
            + "|       7|     7.1|         3|       2|         1|       3|         4|\n"
            + "|       7|     7.1|         3|       4|         3|       1|         2|\n"
            + "|       7|     7.1|         3|       4|         3|       2|         3|\n"
            + "|       7|     7.1|         3|       4|         3|       3|         4|\n"
            + "+--------+--------+----------+--------+----------+--------+----------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a INNER JOIN (SELECT a FROM test.b) ON test.a.a < test.b.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.key|\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "|       1|     1.1|         1|       2|         1|\n"
            + "|       1|     1.1|         1|       4|         3|\n"
            + "|       1|     1.1|         1|       6|         5|\n"
            + "|       3|     3.1|         2|       4|         3|\n"
            + "|       3|     3.1|         2|       6|         5|\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a LEFT OUTER JOIN (SELECT a FROM test.b) ON test.a.a < test.b.a";
    expected =
        "ResultSets:\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.key|test.b.a|test.b.key|\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "|       1|     1.1|         1|       2|         1|\n"
            + "|       1|     1.1|         1|       4|         3|\n"
            + "|       1|     1.1|         1|       6|         5|\n"
            + "|       3|     3.1|         2|       4|         3|\n"
            + "|       3|     3.1|         2|       6|         5|\n"
            + "|       7|     7.1|         3|    null|      null|\n"
            + "+--------+--------+----------+--------+----------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a, (SELECT * FROM (SELECT a FROM test.b WHERE test.b.a < 6), (SELECT b FROM test.c WHERE test.c.b = 1)) AS sub;";
    expected =
        "ResultSets:\n"
            + "+------------+--------------+------------+--------------+--------+--------+----------+\n"
            + "|sub.test.b.a|sub.test.b.key|sub.test.c.b|sub.test.c.key|test.a.a|test.a.b|test.a.key|\n"
            + "+------------+--------------+------------+--------------+--------+--------+----------+\n"
            + "|           2|             1|           1|             2|       1|     1.1|         1|\n"
            + "|           4|             3|           1|             2|       1|     1.1|         1|\n"
            + "|           2|             1|           1|             2|       3|     3.1|         2|\n"
            + "|           4|             3|           1|             2|       3|     3.1|         2|\n"
            + "|           2|             1|           1|             2|       7|     7.1|         3|\n"
            + "|           4|             3|           1|             2|       7|     7.1|         3|\n"
            + "+------------+--------------+------------+--------------+--------+--------+----------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testSelectSubQuery() {
    String insert =
        "INSERT INTO test.a(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val7\"), (4, 3, 2, 2.1, \"val8\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.b(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val2\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.c(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val4\"), (5, 1, 2, 3.1, \"val5\"), (6, 2, 2, 5.1, \"val6\");";
    executor.execute(insert);

    String statement = "SELECT a FROM test.a;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|test.a.a|\n"
            + "+---+--------+\n"
            + "|  1|       3|\n"
            + "|  2|       1|\n"
            + "|  3|       2|\n"
            + "|  4|       3|\n"
            + "|  5|       1|\n"
            + "|  6|       2|\n"
            + "+---+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a FROM test.b;";
    expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|test.b.a|\n"
            + "+---+--------+\n"
            + "|  1|       3|\n"
            + "|  2|       1|\n"
            + "|  3|       2|\n"
            + "|  4|       3|\n"
            + "|  5|       1|\n"
            + "|  6|       2|\n"
            + "+---+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, (SELECT SUM(a) FROM test.b WHERE test.a.a < test.b.a) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-------------+\n"
            + "|key|test.a.a|sum(test.b.a)|\n"
            + "+---+--------+-------------+\n"
            + "|  1|       3|            0|\n"
            + "|  2|       1|           10|\n"
            + "|  3|       2|            6|\n"
            + "|  4|       3|            0|\n"
            + "|  5|       1|           10|\n"
            + "|  6|       2|            6|\n"
            + "+---+--------+-------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, a * (SELECT SUM(a) FROM test.b WHERE test.a.a < test.b.a) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+------------------------+\n"
            + "|key|test.a.a|test.a.a × sum(test.b.a)|\n"
            + "+---+--------+------------------------+\n"
            + "|  1|       3|                       0|\n"
            + "|  2|       1|                      10|\n"
            + "|  3|       2|                      12|\n"
            + "|  4|       3|                       0|\n"
            + "|  5|       1|                      10|\n"
            + "|  6|       2|                      12|\n"
            + "+---+--------+------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, (SELECT AVG(a) FROM test.b) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-------------+\n"
            + "|key|test.a.a|avg(test.b.a)|\n"
            + "+---+--------+-------------+\n"
            + "|  1|       3|          2.0|\n"
            + "|  2|       1|          2.0|\n"
            + "|  3|       2|          2.0|\n"
            + "|  4|       3|          2.0|\n"
            + "|  5|       1|          2.0|\n"
            + "|  6|       2|          2.0|\n"
            + "+---+--------+-------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, (SELECT AVG(a) FROM test.b WHERE test.a.a < test.b.a) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-------------+\n"
            + "|key|test.a.a|avg(test.b.a)|\n"
            + "+---+--------+-------------+\n"
            + "|  1|       3|          NaN|\n"
            + "|  2|       1|          2.5|\n"
            + "|  3|       2|          3.0|\n"
            + "|  4|       3|          NaN|\n"
            + "|  5|       1|          2.5|\n"
            + "|  6|       2|          3.0|\n"
            + "+---+--------+-------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, (SELECT AVG(a) FROM test.b) FROM test.a WHERE a > 1;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-------------+\n"
            + "|key|test.a.a|avg(test.b.a)|\n"
            + "+---+--------+-------------+\n"
            + "|  1|       3|          2.0|\n"
            + "|  3|       2|          2.0|\n"
            + "|  4|       3|          2.0|\n"
            + "|  6|       2|          2.0|\n"
            + "+---+--------+-------------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT d, AVG(a) FROM test.b GROUP BY d HAVING avg(a) > 2;";
    expected =
        "ResultSets:\n"
            + "+--------+-------------+\n"
            + "|test.b.d|avg(test.b.a)|\n"
            + "+--------+-------------+\n"
            + "|    val1|          3.0|\n"
            + "+--------+-------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT a, (SELECT d, AVG(a) FROM test.b GROUP BY d HAVING avg(test.b.a) > 2) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+-------------+\n"
            + "|key|test.a.a|test.b.d|avg(test.b.a)|\n"
            + "+---+--------+--------+-------------+\n"
            + "|  1|       3|    val1|          3.0|\n"
            + "|  2|       1|    val1|          3.0|\n"
            + "|  3|       2|    val1|          3.0|\n"
            + "|  4|       3|    val1|          3.0|\n"
            + "|  5|       1|    val1|          3.0|\n"
            + "|  6|       2|    val1|          3.0|\n"
            + "+---+--------+--------+-------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, 1 + (SELECT AVG(a) FROM test.b) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-----------------+\n"
            + "|key|test.a.a|1 + avg(test.b.a)|\n"
            + "+---+--------+-----------------+\n"
            + "|  1|       3|              3.0|\n"
            + "|  2|       1|              3.0|\n"
            + "|  3|       2|              3.0|\n"
            + "|  4|       3|              3.0|\n"
            + "|  5|       1|              3.0|\n"
            + "|  6|       2|              3.0|\n"
            + "+---+--------+-----------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, 1 + (SELECT AVG(a) FROM test.b WHERE test.a.a < test.b.a) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-----------------+\n"
            + "|key|test.a.a|1 + avg(test.b.a)|\n"
            + "+---+--------+-----------------+\n"
            + "|  1|       3|              NaN|\n"
            + "|  2|       1|              3.5|\n"
            + "|  3|       2|              4.0|\n"
            + "|  4|       3|              NaN|\n"
            + "|  5|       1|              3.5|\n"
            + "|  6|       2|              4.0|\n"
            + "+---+--------+-----------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a / (SELECT AVG(a) FROM test.b) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+------------------------+\n"
            + "|key|test.a.a ÷ avg(test.b.a)|\n"
            + "+---+------------------------+\n"
            + "|  1|                     1.5|\n"
            + "|  2|                     0.5|\n"
            + "|  3|                     1.0|\n"
            + "|  4|                     1.5|\n"
            + "|  5|                     0.5|\n"
            + "|  6|                     1.0|\n"
            + "+---+------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a / (SELECT AVG(a) FROM test.b WHERE test.a.a < test.b.a) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+------------------------+\n"
            + "|key|test.a.a ÷ avg(test.b.a)|\n"
            + "+---+------------------------+\n"
            + "|  1|                     NaN|\n"
            + "|  2|                     0.4|\n"
            + "|  3|      0.6666666666666666|\n"
            + "|  4|                     NaN|\n"
            + "|  5|                     0.4|\n"
            + "|  6|      0.6666666666666666|\n"
            + "+---+------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a / (1 + (SELECT AVG(a) FROM test.b)) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+------------------------------+\n"
            + "|key|test.a.a ÷ (1 + avg(test.b.a))|\n"
            + "+---+------------------------------+\n"
            + "|  1|                           1.0|\n"
            + "|  2|            0.3333333333333333|\n"
            + "|  3|            0.6666666666666666|\n"
            + "|  4|                           1.0|\n"
            + "|  5|            0.3333333333333333|\n"
            + "|  6|            0.6666666666666666|\n"
            + "+---+------------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT a / (1 + (SELECT AVG(a) FROM test.b WHERE test.a.a < test.b.a)) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+------------------------------+\n"
            + "|key|test.a.a ÷ (1 + avg(test.b.a))|\n"
            + "+---+------------------------------+\n"
            + "|  1|                           NaN|\n"
            + "|  2|            0.2857142857142857|\n"
            + "|  3|                           0.5|\n"
            + "|  4|                           NaN|\n"
            + "|  5|            0.2857142857142857|\n"
            + "|  6|                           0.5|\n"
            + "+---+------------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT a, (SELECT AVG(a) AS a1 FROM test.b GROUP BY d HAVING avg(test.b.a) > 2) * (SELECT AVG(a) AS a2 FROM test.b) FROM test.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+-------+\n"
            + "|key|test.a.a|a1 × a2|\n"
            + "+---+--------+-------+\n"
            + "|  1|       3|    6.0|\n"
            + "|  2|       1|    6.0|\n"
            + "|  3|       2|    6.0|\n"
            + "|  4|       3|    6.0|\n"
            + "|  5|       1|    6.0|\n"
            + "|  6|       2|    6.0|\n"
            + "+---+--------+-------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT test.a.a, test.c.a FROM test.a INNER JOIN test.c ON test.a.d = test.c.d";
    expected =
        "ResultSets:\n"
            + "+--------+--------+\n"
            + "|test.a.a|test.c.a|\n"
            + "+--------+--------+\n"
            + "|       3|       3|\n"
            + "|       1|       1|\n"
            + "|       1|       3|\n"
            + "|       2|       2|\n"
            + "+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT test.a.a, test.c.a, (SELECT AVG(a) FROM test.b) FROM test.a INNER JOIN test.c ON test.a.d = test.c.d";
    expected =
        "ResultSets:\n"
            + "+--------+--------+-------------+\n"
            + "|test.a.a|test.c.a|avg(test.b.a)|\n"
            + "+--------+--------+-------------+\n"
            + "|       3|       3|          2.0|\n"
            + "|       1|       1|          2.0|\n"
            + "|       1|       3|          2.0|\n"
            + "|       2|       2|          2.0|\n"
            + "+--------+--------+-------------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testWhereSubQuery() {
    String insert =
        "INSERT INTO test.a(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val7\"), (4, 3, 2, 2.1, \"val8\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.b(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val2\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.c(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val4\"), (5, 1, 2, 3.1, \"val5\"), (6, 2, 2, 5.1, \"val6\");";
    executor.execute(insert);

    String statement = "SELECT * FROM test.a;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.b;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.b.a|test.b.b|test.b.c|test.b.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  3|       2|       2|     1.1|    val3|\n"
            + "|  4|       3|       2|     2.1|    val2|\n"
            + "|  5|       1|       2|     3.1|    val2|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.c;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.c.a|test.c.b|test.c.c|test.c.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  3|       2|       2|     1.1|    val3|\n"
            + "|  4|       3|       2|     2.1|    val4|\n"
            + "|  5|       1|       2|     3.1|    val5|\n"
            + "|  6|       2|       2|     5.1|    val6|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a WHERE EXISTS (SELECT * FROM test.b WHERE test.b.d = \"val2\");";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a WHERE EXISTS (SELECT * FROM test.b WHERE test.b.d = test.a.d);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a WHERE NOT EXISTS (SELECT * FROM test.b WHERE test.b.d = \"val4\");";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a WHERE NOT EXISTS (SELECT * FROM test.b WHERE test.b.d = test.a.d);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a WHERE EXISTS (SELECT * FROM test.b WHERE test.b.d = \"val4\") OR d = \"val1\";";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE d IN (SELECT d FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE d IN (SELECT d FROM test.b ORDER BY key LIMIT 2);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE d IN (SELECT d FROM test.b WHERE test.b.a = test.a.a);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE d NOT IN (SELECT d FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a WHERE d NOT IN (SELECT d FROM test.b WHERE test.b.a = test.a.a);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE d = SOME (SELECT d FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE d = SOME (SELECT d FROM test.b ORDER BY key LIMIT 2);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE c > SOME (SELECT c FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE d != ALL (SELECT d FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE c >= ALL (SELECT c FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE a = (SELECT AVG(a) FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE a > (SELECT AVG(a) FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test.a WHERE a < (SELECT AVG(a) FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test.a WHERE (SELECT AVG(a) FROM test.c) = (SELECT AVG(a) FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|test.a.d|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|       3|       2|     3.1|    val1|\n"
            + "|  2|       1|       3|     2.1|    val2|\n"
            + "|  3|       2|       2|     1.1|    val7|\n"
            + "|  4|       3|       2|     2.1|    val8|\n"
            + "|  5|       1|       2|     3.1|    val1|\n"
            + "|  6|       2|       2|     5.1|    val3|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testWhereSubQueryWithJoin() {
    String insert =
        "INSERT INTO test.a(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val7\"), (4, 3, 2, 2.1, \"val8\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.b(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val2\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.c(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val4\"), (5, 1, 2, 3.1, \"val5\"), (6, 2, 2, 5.1, \"val6\");";
    executor.execute(insert);

    String statement = "SELECT test.* FROM test.a JOIN test.c ON test.a.d = test.c.d;";
    String expected =
        "ResultSets:\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.c|test.a.d|test.a.key|test.c.a|test.c.b|test.c.c|test.c.d|test.c.key|\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "|       3|       2|     3.1|    val1|         1|       3|       2|     3.1|    val1|         1|\n"
            + "|       1|       3|     2.1|    val2|         2|       1|       3|     2.1|    val2|         2|\n"
            + "|       1|       2|     3.1|    val1|         5|       3|       2|     3.1|    val1|         1|\n"
            + "|       2|       2|     5.1|    val3|         6|       2|       2|     1.1|    val3|         3|\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT test.* FROM test.a JOIN test.c ON test.a.d = test.c.d WHERE EXISTS (SELECT * FROM test.b WHERE test.b.d = \"val4\") OR test.a.d = \"val1\";";
    expected =
        "ResultSets:\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.c|test.a.d|test.a.key|test.c.a|test.c.b|test.c.c|test.c.d|test.c.key|\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "|       3|       2|     3.1|    val1|         1|       3|       2|     3.1|    val1|         1|\n"
            + "|       1|       2|     3.1|    val1|         5|       3|       2|     3.1|    val1|         1|\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT test.* FROM test.a JOIN test.c ON test.a.d = test.c.d WHERE test.a.d IN (SELECT d FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.c|test.a.d|test.a.key|test.c.a|test.c.b|test.c.c|test.c.d|test.c.key|\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "|       3|       2|     3.1|    val1|         1|       3|       2|     3.1|    val1|         1|\n"
            + "|       1|       3|     2.1|    val2|         2|       1|       3|     2.1|    val2|         2|\n"
            + "|       1|       2|     3.1|    val1|         5|       3|       2|     3.1|    val1|         1|\n"
            + "|       2|       2|     5.1|    val3|         6|       2|       2|     1.1|    val3|         3|\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT test.* FROM test.a JOIN test.c ON test.a.d = test.c.d WHERE test.a.a < (SELECT AVG(a) FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "|test.a.a|test.a.b|test.a.c|test.a.d|test.a.key|test.c.a|test.c.b|test.c.c|test.c.d|test.c.key|\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "|       1|       3|     2.1|    val2|         2|       1|       3|     2.1|    val2|         2|\n"
            + "|       1|       2|     3.1|    val1|         5|       3|       2|     3.1|    val1|         1|\n"
            + "+--------+--------+--------+--------+----------+--------+--------+--------+--------+----------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testHavingSubQuery() {
    String insert =
        "INSERT INTO test.a(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.b(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val2\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);

    String statement = "SELECT AVG(a), b FROM test.a GROUP BY b;";
    String expected =
        "ResultSets:\n"
            + "+-------------+--------+\n"
            + "|avg(test.a.a)|test.a.b|\n"
            + "+-------------+--------+\n"
            + "|          2.2|       2|\n"
            + "|          1.0|       3|\n"
            + "+-------------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT AVG(a) FROM test.b;";
    expected =
        "ResultSets:\n"
            + "+-------------+\n"
            + "|avg(test.b.a)|\n"
            + "+-------------+\n"
            + "|          2.0|\n"
            + "+-------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT AVG(a), b FROM test.a GROUP BY b HAVING avg(a) > (SELECT AVG(a) FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+-------------+--------+\n"
            + "|avg(test.a.a)|test.a.b|\n"
            + "+-------------+--------+\n"
            + "|          2.2|       2|\n"
            + "+-------------+--------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testMixedSubQuery() {
    String insert =
        "INSERT INTO test.a(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val7\"), (4, 3, 2, 2.1, \"val8\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.b(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val2\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "INSERT INTO test.c(key, a, b, c, d) VALUES (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), "
            + "(3, 2, 2, 1.1, \"val3\"), (4, 3, 2, 2.1, \"val4\"), (5, 1, 2, 3.1, \"val5\"), (6, 2, 2, 5.1, \"val6\");";
    executor.execute(insert);

    String statement = "SELECT test.a.a FROM (SELECT * FROM test.a);";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|test.a.a|\n"
            + "+---+--------+\n"
            + "|  1|       3|\n"
            + "|  2|       1|\n"
            + "|  3|       2|\n"
            + "|  4|       3|\n"
            + "|  5|       1|\n"
            + "|  6|       2|\n"
            + "+---+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT test.a.a, (SELECT AVG(a) FROM test.b) FROM (SELECT * FROM test.a);";
    expected =
        "ResultSets:\n"
            + "+---+--------+-------------+\n"
            + "|key|test.a.a|avg(test.b.a)|\n"
            + "+---+--------+-------------+\n"
            + "|  1|       3|          2.0|\n"
            + "|  2|       1|          2.0|\n"
            + "|  3|       2|          2.0|\n"
            + "|  4|       3|          2.0|\n"
            + "|  5|       1|          2.0|\n"
            + "|  6|       2|          2.0|\n"
            + "+---+--------+-------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT test.a.a, test.a.a * (SELECT AVG(a) FROM test.b) FROM (SELECT * FROM test.a);";
    expected =
        "ResultSets:\n"
            + "+---+--------+------------------------+\n"
            + "|key|test.a.a|test.a.a × avg(test.b.a)|\n"
            + "+---+--------+------------------------+\n"
            + "|  1|       3|                     6.0|\n"
            + "|  2|       1|                     2.0|\n"
            + "|  3|       2|                     4.0|\n"
            + "|  4|       3|                     6.0|\n"
            + "|  5|       1|                     2.0|\n"
            + "|  6|       2|                     4.0|\n"
            + "+---+--------+------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT test.a.a, test.a.d FROM (SELECT * FROM test.a) WHERE test.a.d IN (SELECT d FROM test.b);";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|test.a.a|test.a.d|\n"
            + "+---+--------+--------+\n"
            + "|  1|       3|    val1|\n"
            + "|  2|       1|    val2|\n"
            + "|  5|       1|    val1|\n"
            + "|  6|       2|    val3|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT test.a.a, test.a.d FROM (SELECT * FROM test.a) WHERE test.a.d IN (SELECT d FROM test.b) OR test.a.a > 2;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|test.a.a|test.a.d|\n"
            + "+---+--------+--------+\n"
            + "|  1|       3|    val1|\n"
            + "|  2|       1|    val2|\n"
            + "|  4|       3|    val8|\n"
            + "|  5|       1|    val1|\n"
            + "|  6|       2|    val3|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT test.a.a, test.a.a * (SELECT AVG(a) FROM test.b) FROM (SELECT * FROM test.a) WHERE test.a.d IN (SELECT d FROM test.b) OR test.a.a > 2;";
    expected =
        "ResultSets:\n"
            + "+---+--------+------------------------+\n"
            + "|key|test.a.a|test.a.a × avg(test.b.a)|\n"
            + "+---+--------+------------------------+\n"
            + "|  1|       3|                     6.0|\n"
            + "|  2|       1|                     2.0|\n"
            + "|  4|       3|                     6.0|\n"
            + "|  5|       1|                     2.0|\n"
            + "|  6|       2|                     4.0|\n"
            + "+---+--------+------------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testSetOperators() {
    String insert =
        "INSERT INTO test(key, a.a, a.b, a.c) VALUES (1, 1, \"aaa\", true), (2, 1, \"eee\", false), (3, 4, \"ccc\", true), (5, 6, \"eee\", false);";
    executor.execute(insert);
    insert =
        "INSERT INTO test(key, b.a, b.b, b.c) VALUES (2, \"eee\", 1, true), (3, \"ccc\", 4, true), (5, \"eee\", 6, false);";
    executor.execute(insert);

    String statement =
        "SELECT a, b, c FROM test.a UNION ALL SELECT b, a, c FROM test.b ORDER BY KEY;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|\n"
            + "+---+--------+--------+--------+\n"
            + "|  1|       1|     aaa|    true|\n"
            + "|  2|       1|     eee|   false|\n"
            + "|  2|       1|     eee|    true|\n"
            + "|  3|       4|     ccc|    true|\n"
            + "|  3|       4|     ccc|    true|\n"
            + "|  5|       6|     eee|   false|\n"
            + "|  5|       6|     eee|   false|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 7\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, b, c FROM test.a UNION SELECT b, a, c FROM test.b ORDER BY KEY;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|\n"
            + "+---+--------+--------+--------+\n"
            + "|  1|       1|     aaa|    true|\n"
            + "|  2|       1|     eee|   false|\n"
            + "|  2|       1|     eee|    true|\n"
            + "|  3|       4|     ccc|    true|\n"
            + "|  5|       6|     eee|   false|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, b, c FROM test.a EXCEPT SELECT b, a, c FROM test.b ORDER BY KEY;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|\n"
            + "+---+--------+--------+--------+\n"
            + "|  1|       1|     aaa|    true|\n"
            + "|  2|       1|     eee|   false|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT a, b, c FROM test.a INTERSECT SELECT b, a, c FROM test.b ORDER BY KEY;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.a.c|\n"
            + "+---+--------+--------+--------+\n"
            + "|  3|       4|     ccc|    true|\n"
            + "|  5|       6|     eee|   false|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testValueToMeta() {
    String insert =
        "INSERT INTO test(key, a.a, a.b, b.a, c.b) VALUES "
            + "(1, \"apple\", 871, 232.1, true), (2, \"peach\", 123, 132.5, false), (3, \"banana\", 356, 317.8, true),"
            + "(4, \"cherry\", 621, 456.1, false), (5, \"grape\", 336, 132.5, true), (6, \"dates\", 119, 232.1, false),"
            + "(7, \"melon\", 516, 113.6, true), (8, \"mango\", 458, 232.1, false), (9, \"pear\", 336, 613.1, true);";
    executor.execute(insert);

    insert =
        "INSERT INTO prefix_test(key, suffix, type) VALUES (0, \"a.a\", \"string\"), (1, \"a.b\", \"long\"), (2, \"b.a\", \"double\"), (3, \"c.b\", \"boolean\");";
    executor.execute(insert);

    String statement = "SELECT * FROM prefix_test";
    String expected =
        "ResultSets:\n"
            + "+---+------------------+----------------+\n"
            + "|key|prefix_test.suffix|prefix_test.type|\n"
            + "+---+------------------+----------------+\n"
            + "|  0|               a.a|          string|\n"
            + "|  1|               a.b|            long|\n"
            + "|  2|               b.a|          double|\n"
            + "|  3|               c.b|         boolean|\n"
            + "+---+------------------+----------------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT value2meta(SELECT suffix FROM prefix_test) FROM test;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.b.a|test.c.b|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|   apple|     871|   232.1|    true|\n"
            + "|  2|   peach|     123|   132.5|   false|\n"
            + "|  3|  banana|     356|   317.8|    true|\n"
            + "|  4|  cherry|     621|   456.1|   false|\n"
            + "|  5|   grape|     336|   132.5|    true|\n"
            + "|  6|   dates|     119|   232.1|   false|\n"
            + "|  7|   melon|     516|   113.6|    true|\n"
            + "|  8|   mango|     458|   232.1|   false|\n"
            + "|  9|    pear|     336|   613.1|    true|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT value2meta(SELECT suffix FROM prefix_test) FROM test WHERE a.b > 500;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.b.a|test.c.b|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|   apple|     871|   232.1|    true|\n"
            + "|  4|  cherry|     621|   456.1|   false|\n"
            + "|  7|   melon|     516|   113.6|    true|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT value2meta(SELECT suffix FROM prefix_test) FROM test WHERE c.b = true;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.b.a|test.c.b|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|   apple|     871|   232.1|    true|\n"
            + "|  3|  banana|     356|   317.8|    true|\n"
            + "|  5|   grape|     336|   132.5|    true|\n"
            + "|  7|   melon|     516|   113.6|    true|\n"
            + "|  9|    pear|     336|   613.1|    true|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT value2meta(SELECT suffix FROM prefix_test) FROM test ORDER BY b.a;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.a.a|test.a.b|test.b.a|test.c.b|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  7|   melon|     516|   113.6|    true|\n"
            + "|  2|   peach|     123|   132.5|   false|\n"
            + "|  5|   grape|     336|   132.5|    true|\n"
            + "|  1|   apple|     871|   232.1|    true|\n"
            + "|  6|   dates|     119|   232.1|   false|\n"
            + "|  8|   mango|     458|   232.1|   false|\n"
            + "|  3|  banana|     356|   317.8|    true|\n"
            + "|  4|  cherry|     621|   456.1|   false|\n"
            + "|  9|    pear|     336|   613.1|    true|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT value2meta(SELECT suffix FROM prefix_test WHERE type = \"string\" OR type = \"long\") FROM test;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|test.a.a|test.a.b|\n"
            + "+---+--------+--------+\n"
            + "|  1|   apple|     871|\n"
            + "|  2|   peach|     123|\n"
            + "|  3|  banana|     356|\n"
            + "|  4|  cherry|     621|\n"
            + "|  5|   grape|     336|\n"
            + "|  6|   dates|     119|\n"
            + "|  7|   melon|     516|\n"
            + "|  8|   mango|     458|\n"
            + "|  9|    pear|     336|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT value2meta(SELECT suffix FROM prefix_test WHERE type = \"double\" OR type = \"boolean\") FROM test WHERE c.b = false;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|test.b.a|test.c.b|\n"
            + "+---+--------+--------+\n"
            + "|  2|   132.5|   false|\n"
            + "|  4|   456.1|   false|\n"
            + "|  6|   232.1|   false|\n"
            + "|  8|   232.1|   false|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT value2meta(SELECT suffix FROM prefix_test WHERE type = \"boolean\") FROM test GROUP BY c.b;";
    expected =
        "ResultSets:\n"
            + "+--------+\n"
            + "|test.c.b|\n"
            + "+--------+\n"
            + "|   false|\n"
            + "|    true|\n"
            + "+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testDateFormat() {
    if (!isAbleToDelete) {
      return;
    }
    String insert = "INSERT INTO us.d2(key, date) VALUES (%s, %s);";
    List<String> dateFormats =
        Arrays.asList(
            "2021-08-26 16:15:27",
            "2021/08/26 16:15:28",
            "2021.08.26 16:15:29",
            "2021-08-26T16:15:30",
            "2021/08/26T16:15:31",
            "2021.08.26T16:15:32",
            "2021-08-26 16:15:27.001",
            "2021/08/26 16:15:28.001",
            "2021.08.26 16:15:29.001",
            "2021-08-26T16:15:30.001",
            "2021/08/26T16:15:31.001",
            "2021.08.26T16:15:32.001");

    for (int i = 0; i < dateFormats.size(); i++) {
      executor.execute(String.format(insert, dateFormats.get(i), i));
    }

    String query = "SELECT date FROM us.d2;";
    String expected =
        "ResultSets:\n"
            + "+-------------------+----------+\n"
            + "|                key|us.d2.date|\n"
            + "+-------------------+----------+\n"
            + "|1629965727000000000|         0|\n"
            + "|1629965727001000000|         6|\n"
            + "|1629965728000000000|         1|\n"
            + "|1629965728001000000|         7|\n"
            + "|1629965729000000000|         2|\n"
            + "|1629965729001000000|         8|\n"
            + "|1629965730000000000|         3|\n"
            + "|1629965730001000000|         9|\n"
            + "|1629965731000000000|         4|\n"
            + "|1629965731001000000|        10|\n"
            + "|1629965732000000000|         5|\n"
            + "|1629965732001000000|        11|\n"
            + "+-------------------+----------+\n"
            + "Total line number = 12\n";
    executor.executeAndCompare(query, expected);

    query =
        "SELECT date FROM us.d2 WHERE key >= 2021-08-26 16:15:27 AND key <= 2021.08.26T16:15:32.001;";
    expected =
        "ResultSets:\n"
            + "+-------------------+----------+\n"
            + "|                key|us.d2.date|\n"
            + "+-------------------+----------+\n"
            + "|1629965727000000000|         0|\n"
            + "|1629965727001000000|         6|\n"
            + "|1629965728000000000|         1|\n"
            + "|1629965728001000000|         7|\n"
            + "|1629965729000000000|         2|\n"
            + "|1629965729001000000|         8|\n"
            + "|1629965730000000000|         3|\n"
            + "|1629965730001000000|         9|\n"
            + "|1629965731000000000|         4|\n"
            + "|1629965731001000000|        10|\n"
            + "|1629965732000000000|         5|\n"
            + "|1629965732001000000|        11|\n"
            + "+-------------------+----------+\n"
            + "Total line number = 12\n";
    executor.executeAndCompare(query, expected);

    query =
        "SELECT date FROM us.d2 WHERE key >= 2021.08.26 16:15:29 AND key <= 2021-08-26T16:15:30.001;";
    expected =
        "ResultSets:\n"
            + "+-------------------+----------+\n"
            + "|                key|us.d2.date|\n"
            + "+-------------------+----------+\n"
            + "|1629965729000000000|         2|\n"
            + "|1629965729001000000|         8|\n"
            + "|1629965730000000000|         3|\n"
            + "|1629965730001000000|         9|\n"
            + "+-------------------+----------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query =
        "SELECT date FROM us.d2 WHERE key >= 2021/08/26 16:15:28 AND key <= 2021/08/26T16:15:31.001;";
    expected =
        "ResultSets:\n"
            + "+-------------------+----------+\n"
            + "|                key|us.d2.date|\n"
            + "+-------------------+----------+\n"
            + "|1629965728000000000|         1|\n"
            + "|1629965728001000000|         7|\n"
            + "|1629965729000000000|         2|\n"
            + "|1629965729001000000|         8|\n"
            + "|1629965730000000000|         3|\n"
            + "|1629965730001000000|         9|\n"
            + "|1629965731000000000|         4|\n"
            + "|1629965731001000000|        10|\n"
            + "+-------------------+----------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testInsertWithSubQuery() {
    String insert =
        "INSERT INTO us.d2(key, s1) VALUES (SELECT s1 FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010);";
    executor.execute(insert);

    String query = "SELECT s1 FROM us.d2;";
    String expected =
        "ResultSets:\n"
            + "+----+--------+\n"
            + "| key|us.d2.s1|\n"
            + "+----+--------+\n"
            + "|1000|    1000|\n"
            + "|1001|    1001|\n"
            + "|1002|    1002|\n"
            + "|1003|    1003|\n"
            + "|1004|    1004|\n"
            + "|1005|    1005|\n"
            + "|1006|    1006|\n"
            + "|1007|    1007|\n"
            + "|1008|    1008|\n"
            + "|1009|    1009|\n"
            + "+----+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(query, expected);

    insert =
        "INSERT INTO us.d3(key, s1) VALUES (SELECT s1 FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010) TIME_OFFSET = 100;";
    executor.execute(insert);

    query = "SELECT s1 FROM us.d3;";
    expected =
        "ResultSets:\n"
            + "+----+--------+\n"
            + "| key|us.d3.s1|\n"
            + "+----+--------+\n"
            + "|1100|    1000|\n"
            + "|1101|    1001|\n"
            + "|1102|    1002|\n"
            + "|1103|    1003|\n"
            + "|1104|    1004|\n"
            + "|1105|    1005|\n"
            + "|1106|    1006|\n"
            + "|1107|    1007|\n"
            + "|1108|    1008|\n"
            + "|1109|    1009|\n"
            + "+----+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(query, expected);

    insert =
        "INSERT INTO us.d4(key, s1, s2) VALUES (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 OVER (RANGE 10 IN [1000, 1100)));";
    executor.execute(insert);

    query = "SELECT s1, s2 FROM us.d4";
    expected =
        "ResultSets:\n"
            + "+----+--------+--------+\n"
            + "| key|us.d4.s1|us.d4.s2|\n"
            + "+----+--------+--------+\n"
            + "|1000|  1004.5|   10055|\n"
            + "|1010|  1014.5|   10155|\n"
            + "|1020|  1024.5|   10255|\n"
            + "|1030|  1034.5|   10355|\n"
            + "|1040|  1044.5|   10455|\n"
            + "|1050|  1054.5|   10555|\n"
            + "|1060|  1064.5|   10655|\n"
            + "|1070|  1074.5|   10755|\n"
            + "|1080|  1084.5|   10855|\n"
            + "|1090|  1094.5|   10955|\n"
            + "+----+--------+--------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(query, expected);

    insert =
        "INSERT INTO us.d5(key, s1, s2) VALUES (SELECT avg_s1, sum_s2 "
            + "FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 "
            + "FROM us.d1 OVER (RANGE 10 IN [1000, 1100))) "
            + "WHERE avg_s1 > 1020 AND sum_s2 < 10800);";
    executor.execute(insert);

    query = "SELECT s1, s2 FROM us.d5";
    expected =
        "ResultSets:\n"
            + "+----+--------+--------+\n"
            + "| key|us.d5.s1|us.d5.s2|\n"
            + "+----+--------+--------+\n"
            + "|1020|  1024.5|   10255|\n"
            + "|1030|  1034.5|   10355|\n"
            + "|1040|  1044.5|   10455|\n"
            + "|1050|  1054.5|   10555|\n"
            + "|1060|  1064.5|   10655|\n"
            + "|1070|  1074.5|   10755|\n"
            + "+----+--------+--------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(query, expected);

    insert =
        "INSERT INTO us.d6(key, s1, s2) VALUES (SELECT MAX(avg_s1), MIN(sum_s2) "
            + "FROM (SELECT avg_s1, sum_s2 "
            + "FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 "
            + "FROM us.d1 OVER (RANGE 10 IN [1000, 1100))) "
            + "WHERE avg_s1 > 1020 AND sum_s2 < 10800));";
    executor.execute(insert);

    query = "SELECT s1, s2 FROM us.d6";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|us.d6.s1|us.d6.s2|\n"
            + "+---+--------+--------+\n"
            + "|  0|  1074.5|   10255|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testChinesePath() {
    if (!isSupportChinesePath) {
      return;
    }

    // Chinese path
    String insert = "INSERT INTO 测试.前缀(key, 后缀) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
    executor.execute(insert);

    String query = "SELECT 后缀 FROM 测试.前缀;";
    String expected =
        "ResultSets:\n"
            + "+---+--------+\n"
            + "|key|测试.前缀.后缀|\n"
            + "+---+--------+\n"
            + "|  1|       1|\n"
            + "|  2|       2|\n"
            + "|  3|       3|\n"
            + "|  4|       4|\n"
            + "|  5|       5|\n"
            + "+---+--------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testNumericalPath() {
    if (!isSupportNumericalPath) {
      return;
    }

    // numerical path
    String insert =
        "INSERT INTO 114514(key, 1919810) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
    executor.execute(insert);

    String query = "SELECT 1919810 FROM 114514;";
    String expected =
        "ResultSets:\n"
            + "+---+--------------+\n"
            + "|key|114514.1919810|\n"
            + "+---+--------------+\n"
            + "|  1|             1|\n"
            + "|  2|             2|\n"
            + "|  3|             3|\n"
            + "|  4|             4|\n"
            + "|  5|             5|\n"
            + "+---+--------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testSpecialCharacterPath() {
    if (!isSupportSpecialCharacterPath) {
      return;
    }

    // IGinX SQL 路径中支持的合法字符
    String insert =
        "INSERT INTO _:@#$~^{}(key, _:@#$~^{}, _:@#$~^) VALUES (1, 1, 2), (2, 2, 3), (3, 3, 4), (4, 4, 4), (5, 5, 5);";
    executor.execute(insert);

    String query = "SELECT _:@#$~^{} FROM _:@#$~^{};";
    String expected =
        "ResultSets:\n"
            + "+---+-------------------+\n"
            + "|key|_:@#$~^{}._:@#$~^{}|\n"
            + "+---+-------------------+\n"
            + "|  1|                  1|\n"
            + "|  2|                  2|\n"
            + "|  3|                  3|\n"
            + "|  4|                  4|\n"
            + "|  5|                  5|\n"
            + "+---+-------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT _:@#$~^{} FROM _:@#$~^{} WHERE _:@#$~^{} >= 2 AND _:@#$~^{} <= 4";
    expected =
        "ResultSets:\n"
            + "+---+-------------------+\n"
            + "|key|_:@#$~^{}._:@#$~^{}|\n"
            + "+---+-------------------+\n"
            + "|  2|                  2|\n"
            + "|  3|                  3|\n"
            + "|  4|                  4|\n"
            + "+---+-------------------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT _:@#$~^{}, _:@#$~^ FROM _:@#$~^{} WHERE _:@#$~^{} < _:@#$~^";
    expected =
        "ResultSets:\n"
            + "+---+-------------------+-----------------+\n"
            + "|key|_:@#$~^{}._:@#$~^{}|_:@#$~^{}._:@#$~^|\n"
            + "+---+-------------------+-----------------+\n"
            + "|  1|                  1|                2|\n"
            + "|  2|                  2|                3|\n"
            + "|  3|                  3|                4|\n"
            + "+---+-------------------+-----------------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testMixSpecialPath() {
    if (!isSupportChinesePath || !isSupportNumericalPath || !isSupportSpecialCharacterPath) {
      return;
    }

    // mix path
    String insert =
        "INSERT INTO 测试.前缀.114514(key, 1919810._:@#$.后缀) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
    executor.execute(insert);

    String query = "SELECT 1919810._:@#$.后缀 FROM 测试.前缀.114514;";
    String expected =
        "ResultSets:\n"
            + "+---+-----------------------------+\n"
            + "|key|测试.前缀.114514.1919810._:@#$.后缀|\n"
            + "+---+-----------------------------+\n"
            + "|  1|                            1|\n"
            + "|  2|                            2|\n"
            + "|  3|                            3|\n"
            + "|  4|                            4|\n"
            + "|  5|                            5|\n"
            + "+---+-----------------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testErrorClause() {
    String errClause =
        "DELETE FROM us.d1.s1 WHERE key > 105 AND key < 115 AND key >= 120 AND key <= 230;";
    executor.executeAndCompareErrMsg(
        errClause, "This clause delete nothing, check your filter again.");

    errClause = "DELETE FROM us.d1.s1 WHERE key > 105 AND key < 115 AND s1 < 10;";
    executor.executeAndCompareErrMsg(errClause, "delete clause can not use value or path filter.");

    errClause = "DELETE FROM us.d1.s1 WHERE key != 105;";
    executor.executeAndCompareErrMsg(errClause, "Not support [!=] in delete clause.");

    errClause = "SELECT s1 FROM us.d1 OVER (RANGE 100 IN (0, 1000));";
    executor.executeAndCompareErrMsg(
        errClause, "Downsample clause cannot be used without aggregate function.");

    errClause = "SELECT last(s1), max(s2) FROM us.d1;";
    executor.executeAndCompareErrMsg(
        errClause, "SetToSet/SetToRow/RowToRow functions can not be mixed in aggregate query.");

    errClause = "SELECT s1 FROM us.d1 OVER (RANGE 100 IN (100, 10));";
    executor.executeAndCompareErrMsg(
        errClause, "start key should be smaller than end key in key interval.");

    errClause = "SELECT last(s1) FROM us.d1 GROUP BY s2;";
    executor.executeAndCompareErrMsg(
        errClause, "Group by can not use SetToSet and RowToRow functions.");

    errClause = "select * from test.a join test.b where a > 0;";
    executor.executeAndCompareErrMsg(errClause, "Unexpected paths' name: [a].");
  }

  @Test
  public void testExplain() {
    if (isScaling) return;
    String explain = "explain select max(s2), min(s1) from us.d1;";
    String expected =
        "ResultSets:\n"
            + "+-------------------+-------------+------------------------------------------------------------+\n"
            + "|       Logical Tree|Operator Type|                                               Operator Info|\n"
            + "+-------------------+-------------+------------------------------------------------------------+\n"
            + "|Reorder            |      Reorder|                          Order: max(us.d1.s2),min(us.d1.s1)|\n"
            + "|  +--Join          |         Join|                                             JoinBy: ordinal|\n"
            + "|    +--SetTransform| SetTransform|Func: {Name: min, FuncType: System, MappingType: SetMapping}|\n"
            + "|      +--Project   |      Project|      Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000000|\n"
            + "|    +--SetTransform| SetTransform|Func: {Name: max, FuncType: System, MappingType: SetMapping}|\n"
            + "|      +--Project   |      Project|      Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000000|\n"
            + "+-------------------+-------------+------------------------------------------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(explain, expected);

    explain = "explain select s1 from us.d1 where s1 > 10 and s1 < 100;";
    expected =
        "ResultSets:\n"
            + "+----------------+-------------+---------------------------------------------+\n"
            + "|    Logical Tree|Operator Type|                                Operator Info|\n"
            + "+----------------+-------------+---------------------------------------------+\n"
            + "|Reorder         |      Reorder|                              Order: us.d1.s1|\n"
            + "|  +--Project    |      Project|                           Patterns: us.d1.s1|\n"
            + "|    +--Select   |       Select|    Filter: (us.d1.s1 > 10 && us.d1.s1 < 100)|\n"
            + "|      +--Project|      Project|Patterns: us.d1.s1, Target DU: unit0000000000|\n"
            + "+----------------+-------------+---------------------------------------------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(explain, expected);

    explain = "explain physical select max(s2), min(s1) from us.d1;";
    logger.info(executor.execute(explain));

    explain = "explain physical select s1 from us.d1 where s1 > 10 and s1 < 100;";
    logger.info(executor.execute(explain));
  }

  @Test
  public void testDeleteTimeSeries() {
    if (!isAbleToDelete || isScaling) {
      return;
    }
    String showTimeSeries = "SHOW COLUMNS us.*;";
    String expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s1|    LONG|\n"
            + "|us.d1.s2|    LONG|\n"
            + "|us.d1.s3|  BINARY|\n"
            + "|us.d1.s4|  DOUBLE|\n"
            + "+--------+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(showTimeSeries, expected);

    String deleteTimeSeries = "DELETE COLUMNS us.d1.s4";
    executor.execute(deleteTimeSeries);

    showTimeSeries = "SHOW COLUMNS us.*;";
    expected =
        "Columns:\n"
            + "+--------+--------+\n"
            + "|    Path|DataType|\n"
            + "+--------+--------+\n"
            + "|us.d1.s1|    LONG|\n"
            + "|us.d1.s2|    LONG|\n"
            + "|us.d1.s3|  BINARY|\n"
            + "+--------+--------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(showTimeSeries, expected);

    String showTimeSeriesData = "SELECT s4 FROM us.d1;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executor.executeAndCompare(showTimeSeriesData, expected);

    deleteTimeSeries = "DELETE COLUMNS us.*";
    executor.execute(deleteTimeSeries);

    showTimeSeries = "SHOW COLUMNS us.*;";
    expected =
        "Columns:\n"
            + "+----+--------+\n"
            + "|Path|DataType|\n"
            + "+----+--------+\n"
            + "+----+--------+\n"
            + "Empty set.\n";
    executor.executeAndCompare(showTimeSeries, expected);

    showTimeSeriesData = "SELECT * FROM *;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executor.executeAndCompare(showTimeSeriesData, expected);

    String countPoints = "COUNT POINTS";
    expected = "Points num: 0\n";
    executor.executeAndCompare(countPoints, expected);
  }

  @Test
  public void testClearData() throws SessionException, ExecutionException {
    if (!isAbleToClearData || isScaling) return;
    clearData();

    String countPoints = "COUNT POINTS;";
    String expected = "Points num: 0\n";
    executor.executeAndCompare(countPoints, expected);

    String showTimeSeries = "SELECT * FROM *;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executor.executeAndCompare(showTimeSeries, expected);
  }

  @Test
  public void testConcurrentDeleteSinglePath() {
    if (!isAbleToDelete) {
      return;
    }
    String deleteFormat = "DELETE FROM us.d1.s1 WHERE key >= %d AND key < %d;";
    int start = 1000, range = 50;

    List<String> deleteStmts = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_NUM; i++) {
      deleteStmts.add(String.format(deleteFormat, start, start + range));
      start += range;
    }
    executor.concurrentExecute(deleteStmts);

    String query = "SELECT s1 FROM us.d1 WHERE key > 995 AND key < 1255";
    String expected =
        "ResultSets:\n"
            + "+----+--------+\n"
            + "| key|us.d1.s1|\n"
            + "+----+--------+\n"
            + "| 996|     996|\n"
            + "| 997|     997|\n"
            + "| 998|     998|\n"
            + "| 999|     999|\n"
            + "|1250|    1250|\n"
            + "|1251|    1251|\n"
            + "|1252|    1252|\n"
            + "|1253|    1253|\n"
            + "|1254|    1254|\n"
            + "+----+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT COUNT(s1) FROM us.d1;";
    expected =
        "ResultSets:\n"
            + "+---------------+\n"
            + "|count(us.d1.s1)|\n"
            + "+---------------+\n"
            + "|          14750|\n"
            + "+---------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testConcurrentDeleteSinglePathWithOverlap() {
    if (!isAbleToDelete) {
      return;
    }
    String deleteFormat = "DELETE FROM * WHERE key >= %d AND key < %d;";
    int start = 1000, range = 70;

    List<String> deleteStmts = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_NUM; i++) {
      deleteStmts.add(String.format(deleteFormat, start, start + range));
      start += range - 20;
    }
    executor.concurrentExecute(deleteStmts);

    String query = "SELECT COUNT(*) FROM us.d1;";
    String expected =
        "ResultSets:\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|          14730|          14730|          14730|          14730|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testConcurrentDeleteMultiPath() {
    if (!isAbleToDelete) {
      return;
    }
    String deleteFormat = "DELETE FROM * WHERE key >= %d AND key < %d;";
    int start = 1000, range = 50;

    List<String> deleteStmts = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_NUM; i++) {
      deleteStmts.add(String.format(deleteFormat, start, start + range));
      start += range;
    }
    executor.concurrentExecute(deleteStmts);

    String query = "SELECT COUNT(*) FROM us.d1;";
    String expected =
        "ResultSets:\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|          14750|          14750|          14750|          14750|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testConcurrentDeleteMultiPathWithOverlap() {
    if (!isAbleToDelete) {
      return;
    }
    String deleteFormat = "DELETE FROM * WHERE key >= %d AND key < %d;";
    int start = 1000, range = 70;

    List<String> deleteStmts = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_NUM; i++) {
      deleteStmts.add(String.format(deleteFormat, start, start + range));
      start += range - 20;
    }
    executor.concurrentExecute(deleteStmts);

    String query = "SELECT COUNT(*) FROM us.d1;";
    String expected =
        "ResultSets:\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|          14730|          14730|          14730|          14730|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testConcurrentInsert() {
    int start = 20000, range = 50;

    List<String> insertStmts = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_NUM; i++) {
      insertStmts.add(generateDefaultInsertStatementByTimeRange(start, start + range));
      start += range;
    }
    executor.concurrentExecute(insertStmts);

    String query = "SELECT COUNT(*) FROM us.d1;";
    String expected =
        "ResultSets:\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|          15250|          15250|          15250|          15250|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testConcurrentInsertWithOverlap() {
    int start = 20000, range = 70;

    List<String> insertStmts = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_NUM; i++) {
      insertStmts.add(generateDefaultInsertStatementByTimeRange(start, start + range));
      start += range - 20;
    }
    executor.concurrentExecute(insertStmts);

    String query = "SELECT COUNT(*) FROM us.d1;";
    String expected =
        "ResultSets:\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "|          15270|          15270|          15270|          15270|\n"
            + "+---------------+---------------+---------------+---------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testBaseInfoConcurrentQuery() {
    if (isScaling) {
      return;
    }
    List<Pair<String, String>> statementsAndExpectRes =
        Arrays.asList(
            new Pair<>(
                "SHOW COLUMNS",
                "Columns:\n"
                    + "+--------+--------+\n"
                    + "|    Path|DataType|\n"
                    + "+--------+--------+\n"
                    + "|us.d1.s1|    LONG|\n"
                    + "|us.d1.s2|    LONG|\n"
                    + "|us.d1.s3|  BINARY|\n"
                    + "|us.d1.s4|  DOUBLE|\n"
                    + "+--------+--------+\n"
                    + "Total line number = 4\n"),
            new Pair<>(
                "SELECT COUNT(*) FROM us.d1;",
                "ResultSets:\n"
                    + "+---------------+---------------+---------------+---------------+\n"
                    + "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n"
                    + "+---------------+---------------+---------------+---------------+\n"
                    + "|          15000|          15000|          15000|          15000|\n"
                    + "+---------------+---------------+---------------+---------------+\n"
                    + "Total line number = 1\n"),
            new Pair<>("COUNT POINTS;", "Points num: 60000\n"));
    executor.concurrentExecuteAndCompare(statementsAndExpectRes);
  }

  @Test
  public void testConcurrentQuery() {
    String insert =
        "insert into test1(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\")";
    executor.execute(insert);
    insert =
        "insert into test2(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\")";
    executor.execute(insert);

    List<Pair<String, String>> statementsAndExpectRes =
        Arrays.asList(
            new Pair<>(
                "SELECT s1 FROM us.d1 WHERE key > 0 AND key < 10000 limit 10;",
                "ResultSets:\n"
                    + "+---+--------+\n"
                    + "|key|us.d1.s1|\n"
                    + "+---+--------+\n"
                    + "|  1|       1|\n"
                    + "|  2|       2|\n"
                    + "|  3|       3|\n"
                    + "|  4|       4|\n"
                    + "|  5|       5|\n"
                    + "|  6|       6|\n"
                    + "|  7|       7|\n"
                    + "|  8|       8|\n"
                    + "|  9|       9|\n"
                    + "| 10|      10|\n"
                    + "+---+--------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "SELECT max(s1), max(s4) FROM us.d1 WHERE key > 300 AND s1 <= 600 OVER (RANGE 100 IN (0, 1000) STEP 50);",
                "ResultSets:\n"
                    + "+---+-------------+-------------+\n"
                    + "|key|max(us.d1.s1)|max(us.d1.s4)|\n"
                    + "+---+-------------+-------------+\n"
                    + "|251|          350|        350.1|\n"
                    + "|301|          400|        400.1|\n"
                    + "|351|          450|        450.1|\n"
                    + "|401|          500|        500.1|\n"
                    + "|451|          550|        550.1|\n"
                    + "|501|          600|        600.1|\n"
                    + "|551|          600|        600.1|\n"
                    + "+---+-------------+-------------+\n"
                    + "Total line number = 7\n"),
            new Pair<>(
                "select avg(test1.a), test2.d from test1 join test2 on test1.a = test2.a group by test2.d",
                "ResultSets:\n"
                    + "+------------+-------+\n"
                    + "|avg(test1.a)|test2.d|\n"
                    + "+------------+-------+\n"
                    + "|         2.0|   val5|\n"
                    + "|         2.0|   val3|\n"
                    + "|         2.0|   val2|\n"
                    + "|         2.0|   val1|\n"
                    + "+------------+-------+\n"
                    + "Total line number = 4\n"),
            new Pair<>(
                "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d",
                "ResultSets:\n"
                    + "+------------+------------+-------+\n"
                    + "|avg(test1.a)|max(test1.c)|test2.d|\n"
                    + "+------------+------------+-------+\n"
                    + "|         2.0|         5.1|   val5|\n"
                    + "|         2.0|         5.1|   val3|\n"
                    + "|         2.0|         3.1|   val2|\n"
                    + "|         2.0|         3.1|   val1|\n"
                    + "+------------+------------+-------+\n"
                    + "Total line number = 4\n"),
            new Pair<>(
                "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d having max(test1.c) > 3.5",
                "ResultSets:\n"
                    + "+------------+------------+-------+\n"
                    + "|avg(test1.a)|max(test1.c)|test2.d|\n"
                    + "+------------+------------+-------+\n"
                    + "|         2.0|         5.1|   val5|\n"
                    + "|         2.0|         5.1|   val3|\n"
                    + "+------------+------------+-------+\n"
                    + "Total line number = 2\n"));
    executor.concurrentExecuteAndCompare(statementsAndExpectRes);
  }

  @Test
  public void testFilterPushDownExplain() {
    MultiConnection session =
        new MultiConnection(
            new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
    try {
      session.openSession();
      String queryOptimizer =
          session.executeSql("SHOW CONFIG \"queryOptimizer\"").getResultInString(false, "");
      if (!queryOptimizer.contains("filter_push_down")) {
        logger.info(
            "Skip SQLSessionIT.testFilterPushDownExplain because filter_push_down optimizer is not open");
        return;
      }
    } catch (SessionException | ExecutionException e) {
      logger.error(e.getMessage());
      return;
    }

    String insert =
        "INSERT INTO us.d2(key, c) VALUES (1, \"asdas\"), (2, \"sadaa\"), (3, \"sadada\"), (4, \"asdad\"), (5, \"deadsa\"), (6, \"dasda\"), (7, \"asdsad\"), (8, \"frgsa\"), (9, \"asdad\");";
    executor.execute(insert);

    StringBuilder builder = new StringBuilder();
    builder.append("INSERT INTO us.d2(key, s1) VALUES ");
    int size = (int) (endKey - startKey);
    for (int i = 0; i < size; i++) {
      builder.append(", (");
      builder.append(startKey + i).append(", ");
      builder.append(i + 5);
      builder.append(")");
    }
    builder.append(";");

    insert = builder.toString();
    executor.execute(insert);

    List<Pair<String, String>> statementsAndExpectRes =
        Arrays.asList(
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < 4;",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|                     Filter: us.d1.s1 < 4|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Join       |         Join|                              JoinBy: key|\n"
                    + "|          +--Select   |       Select|                     Filter: us.d1.s1 < 4|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Project  |      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 9\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < 5 and d1.s2 > 2;",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|   Filter: (us.d1.s1 < 5 && us.d1.s2 > 2)|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Join       |         Join|                              JoinBy: key|\n"
                    + "|          +--Select   |       Select|   Filter: (us.d1.s1 < 5 && us.d1.s2 > 2)|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Project  |      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 9\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < 6 and d2.s1 > 3;",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|   Filter: (us.d1.s1 < 6 && us.d2.s1 > 3)|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Join       |         Join|                              JoinBy: key|\n"
                    + "|          +--Select   |       Select|                     Filter: us.d1.s1 < 6|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Select   |       Select|                     Filter: us.d2.s1 > 3|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < 6 or d2.s1 < 7;\n",
                "ResultSets:\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder             |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project        |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select       |       Select|   Filter: (us.d1.s1 < 6 || us.d2.s1 < 7)|\n"
                    + "|      +--Join       |         Join|                              JoinBy: key|\n"
                    + "|        +--Join     |         Join|                              JoinBy: key|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project  |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 8\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d2.c like \"[a|s]\"",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|             Filter: us.d2.c like \"[a|s]\"|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Join       |         Join|                              JoinBy: key|\n"
                    + "|          +--Project  |      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Select   |       Select|             Filter: us.d2.c like \"[a|s]\"|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 9\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE key < 4;\n",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|                          Filter: key < 4|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Join       |         Join|                              JoinBy: key|\n"
                    + "|          +--Select   |       Select|                          Filter: key < 4|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Select   |       Select|                          Filter: key < 4|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Select     |       Select|                          Filter: key < 4|\n"
                    + "|          +--Project  |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 11\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE key < 5 and key > 1;\n",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|             Filter: (key < 5 && key > 1)|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Join       |         Join|                              JoinBy: key|\n"
                    + "|          +--Select   |       Select|             Filter: (key < 5 && key > 1)|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Select   |       Select|             Filter: (key < 5 && key > 1)|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Select     |       Select|             Filter: (key < 5 && key > 1)|\n"
                    + "|          +--Project  |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 11\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE key < 5 or key > 1003;",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|          Filter: (key < 5 || key > 1003)|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Join       |         Join|                              JoinBy: key|\n"
                    + "|          +--Select   |       Select|          Filter: (key < 5 || key > 1003)|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Select   |       Select|          Filter: (key < 5 || key > 1003)|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Select     |       Select|          Filter: (key < 5 || key > 1003)|\n"
                    + "|          +--Project  |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 11\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < d1.s2;\n",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|              Filter: us.d1.s1 < us.d1.s2|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Join       |         Join|                              JoinBy: key|\n"
                    + "|          +--Select   |       Select|              Filter: us.d1.s1 < us.d1.s2|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Project  |      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 9\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < d2.s1;\n",
                "ResultSets:\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder             |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project        |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select       |       Select|              Filter: us.d1.s1 < us.d2.s1|\n"
                    + "|      +--Join       |         Join|                              JoinBy: key|\n"
                    + "|        +--Join     |         Join|                              JoinBy: key|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project  |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 8\n"),
            new Pair<>(
                "explain SELECT * FROM (SELECT * FROM us WHERE us.d1.s1 < 5) WHERE us.d1.s2 < 5;\n",
                "ResultSets:\n"
                    + "+--------------------------+-------------+-----------------------------------------+\n"
                    + "|              Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+--------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                   |      Reorder|                                 Order: *|\n"
                    + "|  +--Project              |      Project|                              Patterns: *|\n"
                    + "|    +--Select             |       Select|                     Filter: us.d1.s2 < 5|\n"
                    + "|      +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|        +--Select         |       Select|                     Filter: us.d1.s1 < 5|\n"
                    + "|          +--Join         |         Join|                              JoinBy: key|\n"
                    + "|            +--Join       |         Join|                              JoinBy: key|\n"
                    + "|              +--Select   |       Select|   Filter: (us.d1.s2 < 5 && us.d1.s1 < 5)|\n"
                    + "|                +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|              +--Project  |      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|            +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+--------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 11\n"),
            new Pair<>(
                "explain SELECT * FROM (SELECT * FROM us WHERE us.d1.s1 < 5) WHERE us.d2.s1 < 10;",
                "ResultSets:\n"
                    + "+--------------------------+-------------+-----------------------------------------+\n"
                    + "|              Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+--------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                   |      Reorder|                                 Order: *|\n"
                    + "|  +--Project              |      Project|                              Patterns: *|\n"
                    + "|    +--Select             |       Select|                    Filter: us.d2.s1 < 10|\n"
                    + "|      +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|        +--Select         |       Select|                     Filter: us.d1.s1 < 5|\n"
                    + "|          +--Join         |         Join|                              JoinBy: key|\n"
                    + "|            +--Join       |         Join|                              JoinBy: key|\n"
                    + "|              +--Select   |       Select|                     Filter: us.d1.s1 < 5|\n"
                    + "|                +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|              +--Select   |       Select|                    Filter: us.d2.s1 < 10|\n"
                    + "|                +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|            +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+--------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 12\n"));

    executor.concurrentExecuteAndCompare(statementsAndExpectRes);
  }

  @Test
  public void testFilterFragmentOptimizer() {
    MultiConnection session =
        new MultiConnection(
            new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
    try {
      session.openSession();
      String queryOptimizer =
          session.executeSql("SHOW CONFIG \"queryOptimizer\"").getResultInString(false, "");
      if (!queryOptimizer.equals("remove_not,filter_fragment")) {
        logger.info(
            "Skip SQLSessionIT.ttestFilterFragmentOptimizer because optimizer is not remove_not,filter_fragment");
        return;
      }
    } catch (SessionException | ExecutionException e) {
      logger.error(e.getMessage());
      return;
    }

    String insert =
        "INSERT INTO us.d2(key, c) VALUES (1, \"asdas\"), (2, \"sadaa\"), (3, \"sadada\"), (4, \"asdad\"), (5, \"deadsa\"), (6, \"dasda\"), (7, \"asdsad\"), (8, \"frgsa\"), (9, \"asdad\");";
    executor.execute(insert);

    StringBuilder builder = new StringBuilder();
    builder.append("INSERT INTO us.d2(key, s1) VALUES ");
    int size = (int) (endKey - startKey);
    for (int i = 0; i < size; i++) {
      builder.append(", (");
      builder.append(startKey + i).append(", ");
      builder.append(i + 5);
      builder.append(")");
    }
    builder.append(";");

    insert = builder.toString();
    executor.execute(insert);

    List<Pair<String, String>> statementsAndExpectRes =
        Arrays.asList(
            new Pair<>(
                "explain SELECT COUNT(*)\n"
                    + "FROM (\n"
                    + "    SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2\n"
                    + "    FROM us.d1 OVER (RANGE 10 IN [1000, 1100))\n"
                    + ")\n"
                    + "OVER (RANGE 20 IN [1000, 1100));",
                "ResultSets:\n"
                    + "+--------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------+\n"
                    + "|              Logical Tree|Operator Type|                                                                                                            Operator Info|\n"
                    + "+--------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------+\n"
                    + "|Reorder                   |      Reorder|                                                                                                          Order: count(*)|\n"
                    + "|  +--Downsample           |   Downsample|Precision: 20, SlideDistance: 20, TimeRange: [1000, 1100), Func: {Name: count, FuncType: System, MappingType: SetMapping}|\n"
                    + "|    +--Select             |       Select|                                                                                      Filter: (key >= 1000 && key < 1100)|\n"
                    + "|      +--Rename           |       Rename|                                            AliasMap: (sum(us.d1.s2), sum_s2),(avg(us.d1.s1), avg_s1), IgnorePatterns: []|\n"
                    + "|        +--Join           |         Join|                                                                                                              JoinBy: key|\n"
                    + "|          +--Downsample   |   Downsample|  Precision: 10, SlideDistance: 10, TimeRange: [1000, 1100), Func: {Name: avg, FuncType: System, MappingType: SetMapping}|\n"
                    + "|            +--Select     |       Select|                                                                                      Filter: (key >= 1000 && key < 1100)|\n"
                    + "|              +--PathUnion|    PathUnion|                                                                                                                         |\n"
                    + "|                +--Project|      Project|                                                                   Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000001|\n"
                    + "|                +--Project|      Project|                                                                   Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000002|\n"
                    + "|          +--Downsample   |   Downsample|  Precision: 10, SlideDistance: 10, TimeRange: [1000, 1100), Func: {Name: sum, FuncType: System, MappingType: SetMapping}|\n"
                    + "|            +--Select     |       Select|                                                                                      Filter: (key >= 1000 && key < 1100)|\n"
                    + "|              +--PathUnion|    PathUnion|                                                                                                                         |\n"
                    + "|                +--Project|      Project|                                                                   Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000001|\n"
                    + "|                +--Project|      Project|                                                                   Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000002|\n"
                    + "+--------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------+\n"
                    + "Total line number = 15\n"),
            new Pair<>(
                "EXPLAIN SELECT s1 FROM us.d1 JOIN us.d2 WHERE key < 100;",
                "ResultSets:\n"
                    + "+--------------------+-------------+------------------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                                   Operator Info|\n"
                    + "+--------------------+-------------+------------------------------------------------+\n"
                    + "|Reorder             |      Reorder|                                       Order: s1|\n"
                    + "|  +--Project        |      Project|                                    Patterns: s1|\n"
                    + "|    +--Select       |       Select|                               Filter: key < 100|\n"
                    + "|      +--InnerJoin  |    InnerJoin|PrefixA: us.d1, PrefixB: us.d2, IsNatural: false|\n"
                    + "|        +--PathUnion|    PathUnion|                                                |\n"
                    + "|          +--Project|      Project|    Patterns: us.d1.*, Target DU: unit0000000001|\n"
                    + "|          +--Project|      Project|    Patterns: us.d1.*, Target DU: unit0000000002|\n"
                    + "|        +--PathUnion|    PathUnion|                                                |\n"
                    + "|          +--Project|      Project|    Patterns: us.d2.*, Target DU: unit0000000001|\n"
                    + "|          +--Project|      Project|    Patterns: us.d2.*, Target DU: unit0000000002|\n"
                    + "+--------------------+-------------+------------------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "EXPLAIN SELECT avg(bb) FROM (SELECT a as aa, b as bb FROM us.d2) WHERE key > 2 GROUP BY aa;",
                "ResultSets:\n"
                    + "+--------------------+-------------+-----------------------------------------------------------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                                                                            Operator Info|\n"
                    + "+--------------------+-------------+-----------------------------------------------------------------------------------------+\n"
                    + "|Reorder             |      Reorder|                                                                           Order: avg(bb)|\n"
                    + "|  +--GroupBy        |      GroupBy|GroupByCols: aa, FunctionCallList: {Name: avg, FuncType: System, MappingType: SetMapping}|\n"
                    + "|    +--Select       |       Select|                                                                          Filter: key > 2|\n"
                    + "|      +--Rename     |       Rename|                                AliasMap: (us.d2.a, aa),(us.d2.b, bb), IgnorePatterns: []|\n"
                    + "|        +--Project  |      Project|                                                                Patterns: us.d2.a,us.d2.b|\n"
                    + "|          +--Project|      Project|                                     Patterns: us.d2.a,us.d2.b, Target DU: unit0000000001|\n"
                    + "+--------------------+-------------+-----------------------------------------------------------------------------------------+\n"
                    + "Total line number = 6\n"));

    executor.concurrentExecuteAndCompare(statementsAndExpectRes);
  }

  @Test
  public void testFilterWithMultiTable() {
    String insert1 = "INSERT INTO test.a(key, a) VALUES (1, 1), (2, 2), (3, 3)";
    String insert2 = "INSERT INTO test.b(key, a) VALUES (1, 1), (2, 2), (3, 3)";
    String insert3 = "INSERT INTO test.c(key, a) VALUES (1, 1), (2, 2), (3, 3)";

    executor.execute(insert1);
    executor.execute(insert2);
    executor.execute(insert3);

    String query = "SELECT * FROM test WHERE a.a < 3";
    String expect =
        "ResultSets:\n"
            + "+---+--------+--------+--------+\n"
            + "|key|test.a.a|test.b.a|test.c.a|\n"
            + "+---+--------+--------+--------+\n"
            + "|  1|       1|       1|       1|\n"
            + "|  2|       2|       2|       2|\n"
            + "+---+--------+--------+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expect);
  }
}
