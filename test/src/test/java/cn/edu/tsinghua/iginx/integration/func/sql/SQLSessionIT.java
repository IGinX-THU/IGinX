package cn.edu.tsinghua.iginx.integration.func.sql;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.*;
import cn.edu.tsinghua.iginx.integration.tool.DBConf.DBConfType;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
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
  protected static String runningEngine;

  protected static final Logger LOGGER = LoggerFactory.getLogger(SQLSessionIT.class);

  protected static final boolean isOnWin =
      System.getProperty("os.name").toLowerCase().contains("win");

  protected boolean isAbleToDelete;

  protected boolean isSupportChinesePath;

  protected boolean isSupportNumericalPath;

  protected boolean isSupportSpecialCharacterPath;

  protected boolean isAbleToShowColumns;

  protected boolean isScaling = false;

  private final long startKey = 0L;

  private final long endKey = 15000L;

  private boolean isFilterPushDown;

  protected boolean isAbleToClearData = true;
  private static final int CONCURRENT_NUM = 5;

  private static MultiConnection session;

  private static boolean dummyNoData = true;

  protected static boolean needCompareResult = true;

  public SQLSessionIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    runningEngine = conf.getStorageType();
    DBConf dbConf = conf.loadDBConf(runningEngine);
    this.isScaling = conf.isScaling();
    if (!SUPPORT_KEY.get(conf.getStorageType()) && this.isScaling) {
      needCompareResult = false;
      executor.setNeedCompareResult(needCompareResult);
    }
    this.isAbleToClearData = dbConf.getEnumValue(DBConf.DBConfType.isAbleToClearData);
    this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    this.isAbleToShowColumns = dbConf.getEnumValue(DBConf.DBConfType.isAbleToShowColumns);
    this.isSupportChinesePath = dbConf.getEnumValue(DBConfType.isSupportChinesePath);
    this.isSupportNumericalPath = dbConf.getEnumValue(DBConfType.isSupportNumericalPath);
    this.isSupportSpecialCharacterPath =
        dbConf.getEnumValue(DBConfType.isSupportSpecialCharacterPath);

    String rules = executor.execute("SHOW RULES;");
    this.isFilterPushDown = rules.contains("FilterPushOutJoinConditionRule|    ON|");
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    dummyNoData = true;
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
      LOGGER.error("isForSession=false, isForSessionPool=false");
      fail();
      return;
    }
    executor = new SQLExecutor(session);
    executor.open();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    executor.close();
  }

  @Before
  public void insertData() {
    generateData(startKey, endKey);
    Controller.after(session);
  }

  private void generateData(long start, long end) {
    // construct insert statement
    List<String> pathList =
        new ArrayList<String>() {
          {
            add("us.d1.s1");
            add("us.d1.s2");
            add("us.d1.s3");
            add("us.d1.s4");
          }
        };
    List<DataType> dataTypeList =
        new ArrayList<DataType>() {
          {
            add(DataType.LONG);
            add(DataType.LONG);
            add(DataType.BINARY);
            add(DataType.DOUBLE);
          }
        };

    List<Long> keyList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    int size = (int) (end - start);
    for (int i = 0; i < size; i++) {
      keyList.add(start + i);
      valuesList.add(
          Arrays.asList(
              (long) i,
              (long) i + 1,
              ("\"" + RandomStringUtils.randomAlphanumeric(10) + "\"").getBytes(),
              (i + 0.1d)));
    }

    Controller.writeRowsData(
        session,
        pathList,
        keyList,
        dataTypeList,
        valuesList,
        new ArrayList<>(),
        InsertAPIType.Row,
        dummyNoData);
    dummyNoData = false;
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

  private int getTimeCostFromExplainPhysicalResult(String explainPhysicalResult) {
    String[] lines = explainPhysicalResult.split("\n");
    int executeTimeIndex = -1;
    int timeCost = 0;
    for (String line : lines) {
      String[] split = line.split("\\|");
      if (split.length > 1) {
        if (executeTimeIndex == -1) {
          for (int i = 0; i < split.length; i++) {
            if (split[i].trim().contains("Time")) {
              executeTimeIndex = i;
              break;
            }
          }
        } else {
          timeCost += Integer.parseInt(split[executeTimeIndex].trim().replace("ms", ""));
        }
      }
    }
    return timeCost;
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
  public void testShowColumns() {
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

    query = "SELECT c FROM us.d2 WHERE c like \"^[s|f].*\";";
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

    query = "SELECT s1 FROM us.* WHERE s1 &> 200 and s1 &< 210;";
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

    query = "SELECT s1 FROM us.* WHERE s1 > 200 and s1 < 210;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|us.d1.s1|us.d2.s1|\n"
            + "+---+--------+--------+\n"
            + "|196|     196|     201|\n"
            + "|197|     197|     202|\n"
            + "|198|     198|     203|\n"
            + "|199|     199|     204|\n"
            + "|200|     200|     205|\n"
            + "|201|     201|     206|\n"
            + "|202|     202|     207|\n"
            + "|203|     203|     208|\n"
            + "|204|     204|     209|\n"
            + "|205|     205|     210|\n"
            + "|206|     206|     211|\n"
            + "|207|     207|     212|\n"
            + "|208|     208|     213|\n"
            + "|209|     209|     214|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 14\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT s1 FROM us.* WHERE s1 &> 200 and s1 |< 210;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|us.d1.s1|us.d2.s1|\n"
            + "+---+--------+--------+\n"
            + "|201|     201|     206|\n"
            + "|202|     202|     207|\n"
            + "|203|     203|     208|\n"
            + "|204|     204|     209|\n"
            + "|205|     205|     210|\n"
            + "|206|     206|     211|\n"
            + "|207|     207|     212|\n"
            + "|208|     208|     213|\n"
            + "|209|     209|     214|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT s1 FROM us.* WHERE s1 &!= 205 and key >= 200 and key < 210;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|us.d1.s1|us.d2.s1|\n"
            + "+---+--------+--------+\n"
            + "|201|     201|     206|\n"
            + "|202|     202|     207|\n"
            + "|203|     203|     208|\n"
            + "|204|     204|     209|\n"
            + "|206|     206|     211|\n"
            + "|207|     207|     212|\n"
            + "|208|     208|     213|\n"
            + "|209|     209|     214|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT s1 FROM us.* WHERE s1 = 205 and key >= 200 and key < 210;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+\n"
            + "|key|us.d1.s1|us.d2.s1|\n"
            + "+---+--------+--------+\n"
            + "|200|     200|     205|\n"
            + "|205|     205|     210|\n"
            + "+---+--------+--------+\n"
            + "Total line number = 2\n";
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
  public void testExprFilter() {
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

    statement = "SELECT * FROM test WHERE 1 < 2;";
    expected =
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

    statement = "SELECT * FROM test WHERE 1 > 2;";
    expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "+---+------+------+\n"
            + "Empty set.\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test WHERE a = b + 1;";
    expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  2|     2|     1|\n"
            + "|  5|     3|     2|\n"
            + "|  9|     4|     3|\n"
            + "+---+------+------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test WHERE a + b - 2 > a - b + 1;";
    expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  3|     2|     2|\n"
            + "|  5|     3|     2|\n"
            + "|  8|     4|     2|\n"
            + "|  9|     4|     3|\n"
            + "+---+------+------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT * FROM test WHERE (a + b) / 2 + 6 > (a - b) * 2.5 + 3;";
    expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  1|     1|     1|\n"
            + "|  2|     2|     1|\n"
            + "|  3|     2|     2|\n"
            + "|  5|     3|     2|\n"
            + "|  8|     4|     2|\n"
            + "|  9|     4|     3|\n"
            + "+---+------+------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test WHERE a + b - 2 > a - b + 1 and (a + b) / 2 + 6 > (a - b) * 2.5 + 3;";
    expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  3|     2|     2|\n"
            + "|  5|     3|     2|\n"
            + "|  8|     4|     2|\n"
            + "|  9|     4|     3|\n"
            + "+---+------+------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT * FROM test WHERE a + b - 2 > a - b + 1 or (a + b) / 2 + 6 > (a - b) * 2.5 + 3;";
    expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  1|     1|     1|\n"
            + "|  2|     2|     1|\n"
            + "|  3|     2|     2|\n"
            + "|  5|     3|     2|\n"
            + "|  8|     4|     2|\n"
            + "|  9|     4|     3|\n"
            + "+---+------+------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testDistinct() {
    String insert =
        "INSERT INTO test(key, a, b) values (1, 1, 1), (2, 2, 1), (3, 2, 2), (4, 3, 1), (5, 3, 2), (6, 3, 1), (7, 4, 1), (8, 4, 2), (9, 4, 3), (10, 4, 1);";
    executor.execute(insert);

    insert =
        "INSERT INTO t(key, a, b) values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 1), (5, 2, 2), (6, 3, 1);";
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

    statement = "SELECT test.a, t.a FROM test JOIN t ON test.a = t.a;";
    expected =
        "ResultSets:\n"
            + "+------+---+\n"
            + "|test.a|t.a|\n"
            + "+------+---+\n"
            + "|     1|  1|\n"
            + "|     1|  1|\n"
            + "|     1|  1|\n"
            + "|     2|  2|\n"
            + "|     2|  2|\n"
            + "|     2|  2|\n"
            + "|     2|  2|\n"
            + "|     3|  3|\n"
            + "|     3|  3|\n"
            + "|     3|  3|\n"
            + "+------+---+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT DISTINCT test.a, t.a FROM test JOIN t ON test.a = t.a;";
    expected =
        "ResultSets:\n"
            + "+------+---+\n"
            + "|test.a|t.a|\n"
            + "+------+---+\n"
            + "|     1|  1|\n"
            + "|     2|  2|\n"
            + "|     3|  3|\n"
            + "+------+---+\n"
            + "Total line number = 3\n";
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
        "SELECT COUNT(a), AVG(a), SUM(a), MIN(a), MAX(a) FROM test OVER WINDOW (size 2 IN (0, 10]);";
    expected =
        "ResultSets:\n"
            + "+---+------------+----------+-------------+-----------+-----------+-----------+-----------+\n"
            + "|key|window_start|window_end|count(test.a)|avg(test.a)|sum(test.a)|min(test.a)|max(test.a)|\n"
            + "+---+------------+----------+-------------+-----------+-----------+-----------+-----------+\n"
            + "|  1|           1|         2|            2|        1.5|          3|          1|          2|\n"
            + "|  3|           3|         4|            2|        2.5|          5|          2|          3|\n"
            + "|  5|           5|         6|            2|        3.0|          6|          3|          3|\n"
            + "|  7|           7|         8|            2|        4.0|          8|          4|          4|\n"
            + "|  9|           9|        10|            2|        4.0|          8|          4|          4|\n"
            + "+---+------------+----------+-------------+-----------+-----------+-----------+-----------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT COUNT(DISTINCT a), AVG(DISTINCT a), SUM(DISTINCT a), MIN(DISTINCT a), MAX(DISTINCT a) FROM test OVER WINDOW (size 2 IN (0, 10]);";
    expected =
        "ResultSets:\n"
            + "+---+------------+----------+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "|key|window_start|window_end|count(distinct test.a)|avg(distinct test.a)|sum(distinct test.a)|min(distinct test.a)|max(distinct test.a)|\n"
            + "+---+------------+----------+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "|  1|           1|         2|                     2|                 1.5|                   3|                   1|                   2|\n"
            + "|  3|           3|         4|                     2|                 2.5|                   5|                   2|                   3|\n"
            + "|  5|           5|         6|                     1|                 3.0|                   3|                   3|                   3|\n"
            + "|  7|           7|         8|                     1|                 4.0|                   4|                   4|                   4|\n"
            + "|  9|           9|        10|                     1|                 4.0|                   4|                   4|                   4|\n"
            + "+---+------------+----------+----------------------+--------------------+--------------------+--------------------+--------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testDistinctWithNullValues() {
    String insert = "INSERT INTO test(key, a) values (1, 1), (2, 2), (5, 3), (6, 3), (7, 4);";
    executor.execute(insert);

    insert = "INSERT INTO test(key, b) values (1, 1.5), (2, 1.5), (3, 2.5), (4, 2.5);";
    executor.execute(insert);

    insert =
        "INSERT INTO test(key, c) values (3, \"bbb\"), (4, \"bbb\"), (5, \"ccc\"), (6, \"ccc\"), (7, \"ccc\");";
    executor.execute(insert);

    String statement = "SELECT * FROM test;";
    String expected =
        "ResultSets:\n"
            + "+---+------+------+------+\n"
            + "|key|test.a|test.b|test.c|\n"
            + "+---+------+------+------+\n"
            + "|  1|     1|   1.5|  null|\n"
            + "|  2|     2|   1.5|  null|\n"
            + "|  3|  null|   2.5|   bbb|\n"
            + "|  4|  null|   2.5|   bbb|\n"
            + "|  5|     3|  null|   ccc|\n"
            + "|  6|     3|  null|   ccc|\n"
            + "|  7|     4|  null|   ccc|\n"
            + "+---+------+------+------+\n"
            + "Total line number = 7\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT DISTINCT * FROM test;";
    expected =
        "ResultSets:\n"
            + "+------+------+------+\n"
            + "|test.a|test.b|test.c|\n"
            + "+------+------+------+\n"
            + "|     1|   1.5|  null|\n"
            + "|     2|   1.5|  null|\n"
            + "|  null|   2.5|   bbb|\n"
            + "|     3|  null|   ccc|\n"
            + "|     4|  null|   ccc|\n"
            + "+------+------+------+\n"
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

    String orderByQuery = "SELECT * FROM us.d2 ORDER BY KEY;";
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

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s1;";
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

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s1 DESC;";
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

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s3;";
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

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s3, s2;";
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

    orderByQuery = "SELECT * FROM us.d2 ORDER BY s3, s2 DESC;";
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
    String statement = "SELECT %s(s1), %s(s4) FROM us.d1 OVER WINDOW (size 100 IN (0, 1000));";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|  1|           1|       100|          100|        100.1|\n"
                + "|101|         101|       200|          200|        200.1|\n"
                + "|201|         201|       300|          300|        300.1|\n"
                + "|301|         301|       400|          400|        400.1|\n"
                + "|401|         401|       500|          500|        500.1|\n"
                + "|501|         501|       600|          600|        600.1|\n"
                + "|601|         601|       700|          700|        700.1|\n"
                + "|701|         701|       800|          800|        800.1|\n"
                + "|801|         801|       900|          900|        900.1|\n"
                + "|901|         901|      1000|          999|        999.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|  1|           1|       100|            1|          1.1|\n"
                + "|101|         101|       200|          101|        101.1|\n"
                + "|201|         201|       300|          201|        201.1|\n"
                + "|301|         301|       400|          301|        301.1|\n"
                + "|401|         401|       500|          401|        401.1|\n"
                + "|501|         501|       600|          501|        501.1|\n"
                + "|601|         601|       700|          601|        601.1|\n"
                + "|701|         701|       800|          701|        701.1|\n"
                + "|801|         801|       900|          801|        801.1|\n"
                + "|901|         901|      1000|          901|        901.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|key|window_start|window_end|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|  1|           1|       100|                    1|                  1.1|\n"
                + "|101|         101|       200|                  101|                101.1|\n"
                + "|201|         201|       300|                  201|                201.1|\n"
                + "|301|         301|       400|                  301|                301.1|\n"
                + "|401|         401|       500|                  401|                401.1|\n"
                + "|501|         501|       600|                  501|                501.1|\n"
                + "|601|         601|       700|                  601|                601.1|\n"
                + "|701|         701|       800|                  701|                701.1|\n"
                + "|801|         801|       900|                  801|                801.1|\n"
                + "|901|         901|      1000|                  901|                901.1|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|key|window_start|window_end|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|  1|           1|       100|                 100|               100.1|\n"
                + "|101|         101|       200|                 200|               200.1|\n"
                + "|201|         201|       300|                 300|               300.1|\n"
                + "|301|         301|       400|                 400|               400.1|\n"
                + "|401|         401|       500|                 500|               500.1|\n"
                + "|501|         501|       600|                 600|               600.1|\n"
                + "|601|         601|       700|                 700|               700.1|\n"
                + "|701|         701|       800|                 800|               800.1|\n"
                + "|801|         801|       900|                 900|               900.1|\n"
                + "|901|         901|      1000|                 999|               999.1|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|key|window_start|window_end|sum(us.d1.s1)|     sum(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|  1|           1|       100|         5050|            5060.0|\n"
                + "|101|         101|       200|        15050|15060.000000000022|\n"
                + "|201|         201|       300|        25050| 25059.99999999997|\n"
                + "|301|         301|       400|        35050| 35059.99999999994|\n"
                + "|401|         401|       500|        45050| 45059.99999999992|\n"
                + "|501|         501|       600|        55050| 55059.99999999991|\n"
                + "|601|         601|       700|        65050|  65059.9999999999|\n"
                + "|701|         701|       800|        75050| 75059.99999999999|\n"
                + "|801|         801|       900|        85050| 85060.00000000004|\n"
                + "|901|         901|      1000|        94050|  94059.9000000001|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|key|window_start|window_end|avg(us.d1.s1)|     avg(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|  1|           1|       100|         50.5|              50.6|\n"
                + "|101|         101|       200|        150.5|150.60000000000022|\n"
                + "|201|         201|       300|        250.5| 250.5999999999997|\n"
                + "|301|         301|       400|        350.5| 350.5999999999994|\n"
                + "|401|         401|       500|        450.5| 450.5999999999992|\n"
                + "|501|         501|       600|        550.5| 550.5999999999991|\n"
                + "|601|         601|       700|        650.5|  650.599999999999|\n"
                + "|701|         701|       800|        750.5| 750.5999999999999|\n"
                + "|801|         801|       900|        850.5| 850.6000000000005|\n"
                + "|901|         901|      1000|        950.0| 950.1000000000009|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|key|window_start|window_end|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|  1|           1|       100|            100|            100|\n"
                + "|101|         101|       200|            100|            100|\n"
                + "|201|         201|       300|            100|            100|\n"
                + "|301|         301|       400|            100|            100|\n"
                + "|401|         401|       500|            100|            100|\n"
                + "|501|         501|       600|            100|            100|\n"
                + "|601|         601|       700|            100|            100|\n"
                + "|701|         701|       800|            100|            100|\n"
                + "|801|         801|       900|            100|            100|\n"
                + "|901|         901|      1000|             99|             99|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "Total line number = 10\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }

    if (isScaling || isFilterPushDown) {
      return;
    }

    statement =
        "explain SELECT avg(s1), count(s4) FROM us.d1 OVER WINDOW (size 100 IN (0, 1000) SLIDE 50);";
    assertTrue(
        Arrays.stream(executor.execute(statement).split("\\n"))
            .anyMatch(s -> s.contains("Downsample") && s.contains("avg") && s.contains("count")));
  }

  @Test
  public void testRangeDownSampleQuery() {
    String statement =
        "SELECT %s(s1), %s(s4) FROM us.d1 WHERE key > 600 AND s1 <= 900 OVER WINDOW (size 100 IN (0, 1000));";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|601|         601|       700|          700|        700.1|\n"
                + "|701|         701|       800|          800|        800.1|\n"
                + "|801|         801|       900|          900|        900.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|601|         601|       700|          601|        601.1|\n"
                + "|701|         701|       800|          701|        701.1|\n"
                + "|801|         801|       900|          801|        801.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|key|window_start|window_end|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|601|         601|       700|                  601|                601.1|\n"
                + "|701|         701|       800|                  701|                701.1|\n"
                + "|801|         801|       900|                  801|                801.1|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|key|window_start|window_end|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|601|         601|       700|                 700|               700.1|\n"
                + "|701|         701|       800|                 800|               800.1|\n"
                + "|801|         801|       900|                 900|               900.1|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "|key|window_start|window_end|sum(us.d1.s1)|    sum(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "|601|         601|       700|        65050| 65059.9999999999|\n"
                + "|701|         701|       800|        75050|75059.99999999999|\n"
                + "|801|         801|       900|        85050|85060.00000000004|\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "|key|window_start|window_end|avg(us.d1.s1)|    avg(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "|601|         601|       700|        650.5| 650.599999999999|\n"
                + "|701|         701|       800|        750.5|750.5999999999999|\n"
                + "|801|         801|       900|        850.5|850.6000000000005|\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "Total line number = 3\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|key|window_start|window_end|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|601|         601|       700|            100|            100|\n"
                + "|701|         701|       800|            100|            100|\n"
                + "|801|         801|       900|            100|            100|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "Total line number = 3\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }
  }

  @Test
  public void testSlideWindowByTimeQuery() {
    String statement =
        "SELECT %s(s1), %s(s4) FROM us.d1 OVER WINDOW (size 100 IN (0, 1000) SLIDE 50);";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|  1|           1|       100|          100|        100.1|\n"
                + "| 51|          51|       150|          150|        150.1|\n"
                + "|101|         101|       200|          200|        200.1|\n"
                + "|151|         151|       250|          250|        250.1|\n"
                + "|201|         201|       300|          300|        300.1|\n"
                + "|251|         251|       350|          350|        350.1|\n"
                + "|301|         301|       400|          400|        400.1|\n"
                + "|351|         351|       450|          450|        450.1|\n"
                + "|401|         401|       500|          500|        500.1|\n"
                + "|451|         451|       550|          550|        550.1|\n"
                + "|501|         501|       600|          600|        600.1|\n"
                + "|551|         551|       650|          650|        650.1|\n"
                + "|601|         601|       700|          700|        700.1|\n"
                + "|651|         651|       750|          750|        750.1|\n"
                + "|701|         701|       800|          800|        800.1|\n"
                + "|751|         751|       850|          850|        850.1|\n"
                + "|801|         801|       900|          900|        900.1|\n"
                + "|851|         851|       950|          950|        950.1|\n"
                + "|901|         901|      1000|          999|        999.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|  1|           1|       100|            1|          1.1|\n"
                + "| 51|          51|       150|           51|         51.1|\n"
                + "|101|         101|       200|          101|        101.1|\n"
                + "|151|         151|       250|          151|        151.1|\n"
                + "|201|         201|       300|          201|        201.1|\n"
                + "|251|         251|       350|          251|        251.1|\n"
                + "|301|         301|       400|          301|        301.1|\n"
                + "|351|         351|       450|          351|        351.1|\n"
                + "|401|         401|       500|          401|        401.1|\n"
                + "|451|         451|       550|          451|        451.1|\n"
                + "|501|         501|       600|          501|        501.1|\n"
                + "|551|         551|       650|          551|        551.1|\n"
                + "|601|         601|       700|          601|        601.1|\n"
                + "|651|         651|       750|          651|        651.1|\n"
                + "|701|         701|       800|          701|        701.1|\n"
                + "|751|         751|       850|          751|        751.1|\n"
                + "|801|         801|       900|          801|        801.1|\n"
                + "|851|         851|       950|          851|        851.1|\n"
                + "|901|         901|      1000|          901|        901.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|key|window_start|window_end|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|  1|           1|       100|                    1|                  1.1|\n"
                + "| 51|          51|       150|                   51|                 51.1|\n"
                + "|101|         101|       200|                  101|                101.1|\n"
                + "|151|         151|       250|                  151|                151.1|\n"
                + "|201|         201|       300|                  201|                201.1|\n"
                + "|251|         251|       350|                  251|                251.1|\n"
                + "|301|         301|       400|                  301|                301.1|\n"
                + "|351|         351|       450|                  351|                351.1|\n"
                + "|401|         401|       500|                  401|                401.1|\n"
                + "|451|         451|       550|                  451|                451.1|\n"
                + "|501|         501|       600|                  501|                501.1|\n"
                + "|551|         551|       650|                  551|                551.1|\n"
                + "|601|         601|       700|                  601|                601.1|\n"
                + "|651|         651|       750|                  651|                651.1|\n"
                + "|701|         701|       800|                  701|                701.1|\n"
                + "|751|         751|       850|                  751|                751.1|\n"
                + "|801|         801|       900|                  801|                801.1|\n"
                + "|851|         851|       950|                  851|                851.1|\n"
                + "|901|         901|      1000|                  901|                901.1|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|key|window_start|window_end|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|  1|           1|       100|                 100|               100.1|\n"
                + "| 51|          51|       150|                 150|               150.1|\n"
                + "|101|         101|       200|                 200|               200.1|\n"
                + "|151|         151|       250|                 250|               250.1|\n"
                + "|201|         201|       300|                 300|               300.1|\n"
                + "|251|         251|       350|                 350|               350.1|\n"
                + "|301|         301|       400|                 400|               400.1|\n"
                + "|351|         351|       450|                 450|               450.1|\n"
                + "|401|         401|       500|                 500|               500.1|\n"
                + "|451|         451|       550|                 550|               550.1|\n"
                + "|501|         501|       600|                 600|               600.1|\n"
                + "|551|         551|       650|                 650|               650.1|\n"
                + "|601|         601|       700|                 700|               700.1|\n"
                + "|651|         651|       750|                 750|               750.1|\n"
                + "|701|         701|       800|                 800|               800.1|\n"
                + "|751|         751|       850|                 850|               850.1|\n"
                + "|801|         801|       900|                 900|               900.1|\n"
                + "|851|         851|       950|                 950|               950.1|\n"
                + "|901|         901|      1000|                 999|               999.1|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|key|window_start|window_end|sum(us.d1.s1)|     sum(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|  1|           1|       100|         5050|            5060.0|\n"
                + "| 51|          51|       150|        10050|10060.000000000013|\n"
                + "|101|         101|       200|        15050|15060.000000000022|\n"
                + "|151|         151|       250|        20050|20059.999999999996|\n"
                + "|201|         201|       300|        25050| 25059.99999999997|\n"
                + "|251|         251|       350|        30050|30059.999999999953|\n"
                + "|301|         301|       400|        35050| 35059.99999999994|\n"
                + "|351|         351|       450|        40050| 40059.99999999993|\n"
                + "|401|         401|       500|        45050| 45059.99999999992|\n"
                + "|451|         451|       550|        50050| 50059.99999999992|\n"
                + "|501|         501|       600|        55050| 55059.99999999991|\n"
                + "|551|         551|       650|        60050|60059.999999999905|\n"
                + "|601|         601|       700|        65050|  65059.9999999999|\n"
                + "|651|         651|       750|        70050| 70059.99999999994|\n"
                + "|701|         701|       800|        75050| 75059.99999999999|\n"
                + "|751|         751|       850|        80050| 80060.00000000001|\n"
                + "|801|         801|       900|        85050| 85060.00000000004|\n"
                + "|851|         851|       950|        90050| 90060.00000000009|\n"
                + "|901|         901|      1000|        94050|  94059.9000000001|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|key|window_start|window_end|avg(us.d1.s1)|     avg(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|  1|           1|       100|         50.5|              50.6|\n"
                + "| 51|          51|       150|        100.5|100.60000000000012|\n"
                + "|101|         101|       200|        150.5|150.60000000000022|\n"
                + "|151|         151|       250|        200.5|200.59999999999997|\n"
                + "|201|         201|       300|        250.5| 250.5999999999997|\n"
                + "|251|         251|       350|        300.5| 300.5999999999995|\n"
                + "|301|         301|       400|        350.5| 350.5999999999994|\n"
                + "|351|         351|       450|        400.5| 400.5999999999993|\n"
                + "|401|         401|       500|        450.5| 450.5999999999992|\n"
                + "|451|         451|       550|        500.5| 500.5999999999992|\n"
                + "|501|         501|       600|        550.5| 550.5999999999991|\n"
                + "|551|         551|       650|        600.5|  600.599999999999|\n"
                + "|601|         601|       700|        650.5|  650.599999999999|\n"
                + "|651|         651|       750|        700.5| 700.5999999999995|\n"
                + "|701|         701|       800|        750.5| 750.5999999999999|\n"
                + "|751|         751|       850|        800.5| 800.6000000000001|\n"
                + "|801|         801|       900|        850.5| 850.6000000000005|\n"
                + "|851|         851|       950|        900.5| 900.6000000000008|\n"
                + "|901|         901|      1000|        950.0| 950.1000000000009|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "Total line number = 19\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|key|window_start|window_end|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|  1|           1|       100|            100|            100|\n"
                + "| 51|          51|       150|            100|            100|\n"
                + "|101|         101|       200|            100|            100|\n"
                + "|151|         151|       250|            100|            100|\n"
                + "|201|         201|       300|            100|            100|\n"
                + "|251|         251|       350|            100|            100|\n"
                + "|301|         301|       400|            100|            100|\n"
                + "|351|         351|       450|            100|            100|\n"
                + "|401|         401|       500|            100|            100|\n"
                + "|451|         451|       550|            100|            100|\n"
                + "|501|         501|       600|            100|            100|\n"
                + "|551|         551|       650|            100|            100|\n"
                + "|601|         601|       700|            100|            100|\n"
                + "|651|         651|       750|            100|            100|\n"
                + "|701|         701|       800|            100|            100|\n"
                + "|751|         751|       850|            100|            100|\n"
                + "|801|         801|       900|            100|            100|\n"
                + "|851|         851|       950|            100|            100|\n"
                + "|901|         901|      1000|             99|             99|\n"
                + "+---+------------+----------+---------------+---------------+\n"
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
        "SELECT %s(s1), %s(s4) FROM us.d1 WHERE key > 300 AND s1 <= 600 OVER WINDOW (size 100 IN (0, 1000) SLIDE 50);";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|251|         251|       350|          350|        350.1|\n"
                + "|301|         301|       400|          400|        400.1|\n"
                + "|351|         351|       450|          450|        450.1|\n"
                + "|401|         401|       500|          500|        500.1|\n"
                + "|451|         451|       550|          550|        550.1|\n"
                + "|501|         501|       600|          600|        600.1|\n"
                + "|551|         551|       650|          600|        600.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|251|         251|       350|          301|        301.1|\n"
                + "|301|         301|       400|          301|        301.1|\n"
                + "|351|         351|       450|          351|        351.1|\n"
                + "|401|         401|       500|          401|        401.1|\n"
                + "|451|         451|       550|          451|        451.1|\n"
                + "|501|         501|       600|          501|        501.1|\n"
                + "|551|         551|       650|          551|        551.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|key|window_start|window_end|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|251|         251|       350|                  301|                301.1|\n"
                + "|301|         301|       400|                  301|                301.1|\n"
                + "|351|         351|       450|                  351|                351.1|\n"
                + "|401|         401|       500|                  401|                401.1|\n"
                + "|451|         451|       550|                  451|                451.1|\n"
                + "|501|         501|       600|                  501|                501.1|\n"
                + "|551|         551|       650|                  551|                551.1|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|key|window_start|window_end|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|251|         251|       350|                 350|               350.1|\n"
                + "|301|         301|       400|                 400|               400.1|\n"
                + "|351|         351|       450|                 450|               450.1|\n"
                + "|401|         401|       500|                 500|               500.1|\n"
                + "|451|         451|       550|                 550|               550.1|\n"
                + "|501|         501|       600|                 600|               600.1|\n"
                + "|551|         551|       650|                 600|               600.1|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|key|window_start|window_end|sum(us.d1.s1)|     sum(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|251|         251|       350|        16275|16280.000000000013|\n"
                + "|301|         301|       400|        35050| 35059.99999999994|\n"
                + "|351|         351|       450|        40050| 40059.99999999993|\n"
                + "|401|         401|       500|        45050| 45059.99999999992|\n"
                + "|451|         451|       550|        50050| 50059.99999999992|\n"
                + "|501|         501|       600|        55050| 55059.99999999991|\n"
                + "|551|         551|       650|        28775|28779.999999999975|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|key|window_start|window_end|avg(us.d1.s1)|     avg(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "|251|         251|       350|        325.5|325.60000000000025|\n"
                + "|301|         301|       400|        350.5| 350.5999999999994|\n"
                + "|351|         351|       450|        400.5| 400.5999999999993|\n"
                + "|401|         401|       500|        450.5| 450.5999999999992|\n"
                + "|451|         451|       550|        500.5| 500.5999999999992|\n"
                + "|501|         501|       600|        550.5| 550.5999999999991|\n"
                + "|551|         551|       650|        575.5| 575.5999999999995|\n"
                + "+---+------------+----------+-------------+------------------+\n"
                + "Total line number = 7\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|key|window_start|window_end|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|251|         251|       350|             50|             50|\n"
                + "|301|         301|       400|            100|            100|\n"
                + "|351|         351|       450|            100|            100|\n"
                + "|401|         401|       500|            100|            100|\n"
                + "|451|         451|       550|            100|            100|\n"
                + "|501|         501|       600|            100|            100|\n"
                + "|551|         551|       650|             50|             50|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "Total line number = 7\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }
  }

  @Test
  public void testRangeSlideWindowByTimeNoIntervalQuery() {
    String statement =
        "SELECT %s(s1), %s(s4) FROM us.d1 WHERE key > 300 AND s1 <= 600 OVER WINDOW (SIZE 100 SLIDE 50);";
    List<String> funcTypeList =
        Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");
    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|max(us.d1.s1)|max(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|301|         301|       400|          400|        400.1|\n"
                + "|351|         351|       450|          450|        450.1|\n"
                + "|401|         401|       500|          500|        500.1|\n"
                + "|451|         451|       550|          550|        550.1|\n"
                + "|501|         501|       600|          600|        600.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 5\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|key|window_start|window_end|min(us.d1.s1)|min(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "|301|         301|       400|          301|        301.1|\n"
                + "|351|         351|       450|          351|        351.1|\n"
                + "|401|         401|       500|          401|        401.1|\n"
                + "|451|         451|       550|          451|        451.1|\n"
                + "|501|         501|       600|          501|        501.1|\n"
                + "+---+------------+----------+-------------+-------------+\n"
                + "Total line number = 5\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|key|window_start|window_end|first_value(us.d1.s1)|first_value(us.d1.s4)|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "|301|         301|       400|                  301|                301.1|\n"
                + "|351|         351|       450|                  351|                351.1|\n"
                + "|401|         401|       500|                  401|                401.1|\n"
                + "|451|         451|       550|                  451|                451.1|\n"
                + "|501|         501|       600|                  501|                501.1|\n"
                + "+---+------------+----------+---------------------+---------------------+\n"
                + "Total line number = 5\n",
            "ResultSets:\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|key|window_start|window_end|last_value(us.d1.s1)|last_value(us.d1.s4)|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "|301|         301|       400|                 400|               400.1|\n"
                + "|351|         351|       450|                 450|               450.1|\n"
                + "|401|         401|       500|                 500|               500.1|\n"
                + "|451|         451|       550|                 550|               550.1|\n"
                + "|501|         501|       600|                 600|               600.1|\n"
                + "+---+------------+----------+--------------------+--------------------+\n"
                + "Total line number = 5\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "|key|window_start|window_end|sum(us.d1.s1)|    sum(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "|301|         301|       400|        35050|35059.99999999994|\n"
                + "|351|         351|       450|        40050|40059.99999999993|\n"
                + "|401|         401|       500|        45050|45059.99999999992|\n"
                + "|451|         451|       550|        50050|50059.99999999992|\n"
                + "|501|         501|       600|        55050|55059.99999999991|\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "Total line number = 5\n",
            "ResultSets:\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "|key|window_start|window_end|avg(us.d1.s1)|    avg(us.d1.s4)|\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "|301|         301|       400|        350.5|350.5999999999994|\n"
                + "|351|         351|       450|        400.5|400.5999999999993|\n"
                + "|401|         401|       500|        450.5|450.5999999999992|\n"
                + "|451|         451|       550|        500.5|500.5999999999992|\n"
                + "|501|         501|       600|        550.5|550.5999999999991|\n"
                + "+---+------------+----------+-------------+-----------------+\n"
                + "Total line number = 5\n",
            "ResultSets:\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|key|window_start|window_end|count(us.d1.s1)|count(us.d1.s4)|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "|301|         301|       400|            100|            100|\n"
                + "|351|         351|       450|            100|            100|\n"
                + "|401|         401|       500|            100|            100|\n"
                + "|451|         451|       550|            100|            100|\n"
                + "|501|         501|       600|            100|            100|\n"
                + "+---+------------+----------+---------------+---------------+\n"
                + "Total line number = 5\n");
    for (int i = 0; i < funcTypeList.size(); i++) {
      String type = funcTypeList.get(i);
      String expected = expectedList.get(i);
      executor.executeAndCompare(String.format(statement, type, type), expected);
    }
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

    query = "select avg(a), c, b, d from test group by c, b, d order by c, b, d;";
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

    query = "select avg(a), c, b, d from test group by c, b, d order by c, b, d;";
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

    query = "select max(a), avg(b) from test group by c order by c;";
    expected =
        "ResultSets:\n"
            + "+-----------+-----------+\n"
            + "|max(test.a)|avg(test.b)|\n"
            + "+-----------+-----------+\n"
            + "|          2|        2.0|\n"
            + "|          3|        2.5|\n"
            + "|          3|        2.0|\n"
            + "|          2|        2.0|\n"
            + "+-----------+-----------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query = "select avg(a), sum(b), c, b, d from test group by c, b, d order by c, b, d;";
    expected =
        "ResultSets:\n"
            + "+-----------+-----------+------+------+------+\n"
            + "|avg(test.a)|sum(test.b)|test.c|test.b|test.d|\n"
            + "+-----------+-----------+------+------+------+\n"
            + "|        2.0|          2|   1.1|     2|  val5|\n"
            + "|        3.0|          2|   2.1|     2|  val2|\n"
            + "|        1.0|          3|   2.1|     3|  val2|\n"
            + "|        2.0|          4|   3.1|     2|  val1|\n"
            + "|        2.0|          2|   5.1|     2|  val3|\n"
            + "+-----------+-----------+------+------+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    if (isScaling || isFilterPushDown) {
      return;
    }
    query = "explain select avg(a), sum(b), c, b, d from test group by c, b, d order by c, b, d;";
    expected =
        "ResultSets:\n"
            + "+----------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------+\n"
            + "|    Logical Tree|Operator Type|                                                                                                                      Operator Info|\n"
            + "+----------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------+\n"
            + "|Reorder         |      Reorder|                                                                                Order: avg(test.a),sum(test.b),test.c,test.b,test.d|\n"
            + "|  +--Sort       |         Sort|                                                                                        SortBy: test.c,test.b,test.d, SortType: ASC|\n"
            + "|    +--GroupBy  |      GroupBy|GroupByCols: test.c,test.b,test.d, FuncList(Name, FuncType): (avg, System),(sum, System), MappingType: SetMapping isDistinct: false|\n"
            + "|      +--Project|      Project|                                                                   Patterns: test.a,test.b,test.c,test.d, Target DU: unit0000000002|\n"
            + "+----------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------+\n"
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
        "insert into test1(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "insert into test2(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);

    String query = "select * from test1 join test2 on test1.a = test2.a;";
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
        "select avg(test1.a), test2.d from test1 join test2 on test1.a = test2.a group by test2.d order by test2.d desc;";
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
        "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d order by test2.d desc;";
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
        "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d having max(test1.c) > 3.5 order by test2.d desc;";
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
        "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d having max(test1.c) > 3.5 order by test2.d;";
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

    String statement = "select * from test.a join test.b on test.a.a = test.b.a;";
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

    statement = "select * from test.a inner join test.b on test.a.a = test.b.a;";
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

    statement = "select * from test.a left join test.b on test.a.a = test.b.a;";
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

    statement = "select * from test.a left join test.b using a;";
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

    statement = "select * from test.a right join test.b on test.a.a = test.b.a;";
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

    statement = "select * from test.a right join test.b using a;";
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

    statement = "select * from test.a full join test.b on test.a.a = test.b.a;";
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

    statement = "select * from test.a, test.b;";
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

    statement = "select * from test.a, test.b where test.a.a = test.b.a;";
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

    statement = "select * from test.a cross join test.b;";
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

    String statement = "select * from test;";
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
        "select * from test.a join test.b on test.a.a = test.b.a join test.c on test.b.b = test.c.a;";
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
        "select * from test.a, test.b, test.c where test.a.a = test.b.a and test.b.b = test.c.a;";
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

    statement = "select * from test.a full join test.b on test.a.a = test.b.a;";
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
        "select * from test.a full join test.b on test.a.a = test.b.a full join test.c on test.b.b = test.c.a;";
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
            + "|  1|                  6|0.16666666666666666|                  1|\n"
            + "|  2|                 10|                0.4|                  2|\n"
            + "|  3|                 12|               0.75|                  3|\n"
            + "|  4|                 12| 1.3333333333333333|                  1|\n"
            + "|  5|                 10|                2.5|                  1|\n"
            + "|  6|                  6|                6.0|                  0|\n"
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
  public void testSelectFromComplexArithmeticExpr() {
    String insert =
        "INSERT INTO us.d3 (key, s1, s2, s3) VALUES "
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

    statement = "SELECT `(us.d3.s1 + us.d3.s2) × us.d3.s3` FROM (SELECT (s1+s2)*s3 FROM us.d3);";
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

    statement =
        "SELECT `(us.d3.s1 + us.d3.s2) × us.d3.s3` FROM (SELECT (s1+s2)*s3 FROM us.d3) WHERE `(us.d3.s1 + us.d3.s2) × us.d3.s3` < 30;";
    expected =
        "ResultSets:\n"
            + "+---+--------------------------------+\n"
            + "|key|(us.d3.s1 + us.d3.s2) × us.d3.s3|\n"
            + "+---+--------------------------------+\n"
            + "|  1|                            10.5|\n"
            + "|  2|                            17.5|\n"
            + "|  3|                            24.5|\n"
            + "+---+--------------------------------+\n"
            + "Total line number = 3\n";
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

    statement =
        "SELECT * FROM (SELECT s1, s2 FROM us.* WHERE us.d1.s1 >= 1000 AND us.d1.s1 < 1010) AS rename_result_set;";
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
        "SELECT %s_s1 FROM (SELECT %s(s1) AS %s_s1 FROM us.d1 OVER WINDOW(SIZE 60 IN [1000, 1600)));";
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
  public void testSelectFromAggregate() {
    String statement =
        "SELECT `%s(us.d1.s1)` FROM (SELECT %s(s1) FROM us.d1 OVER WINDOW(SIZE 60 IN [1000, 1600)));";
    List<String> funcTypeList =
        Arrays.asList("max", "min", "sum", "avg", "count", "first_value", "last_value");

    List<String> expectedList =
        Arrays.asList(
            "ResultSets:\n"
                + "+----+-------------+\n"
                + "| key|max(us.d1.s1)|\n"
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
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+-------------+\n"
                + "| key|min(us.d1.s1)|\n"
                + "+----+-------------+\n"
                + "|1000|         1000|\n"
                + "|1060|         1060|\n"
                + "|1120|         1120|\n"
                + "|1180|         1180|\n"
                + "|1240|         1240|\n"
                + "|1300|         1300|\n"
                + "|1360|         1360|\n"
                + "|1420|         1420|\n"
                + "|1480|         1480|\n"
                + "|1540|         1540|\n"
                + "+----+-------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+-------------+\n"
                + "| key|sum(us.d1.s1)|\n"
                + "+----+-------------+\n"
                + "|1000|        61770|\n"
                + "|1060|        65370|\n"
                + "|1120|        68970|\n"
                + "|1180|        72570|\n"
                + "|1240|        76170|\n"
                + "|1300|        79770|\n"
                + "|1360|        83370|\n"
                + "|1420|        86970|\n"
                + "|1480|        90570|\n"
                + "|1540|        94170|\n"
                + "+----+-------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+-------------+\n"
                + "| key|avg(us.d1.s1)|\n"
                + "+----+-------------+\n"
                + "|1000|       1029.5|\n"
                + "|1060|       1089.5|\n"
                + "|1120|       1149.5|\n"
                + "|1180|       1209.5|\n"
                + "|1240|       1269.5|\n"
                + "|1300|       1329.5|\n"
                + "|1360|       1389.5|\n"
                + "|1420|       1449.5|\n"
                + "|1480|       1509.5|\n"
                + "|1540|       1569.5|\n"
                + "+----+-------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+---------------+\n"
                + "| key|count(us.d1.s1)|\n"
                + "+----+---------------+\n"
                + "|1000|             60|\n"
                + "|1060|             60|\n"
                + "|1120|             60|\n"
                + "|1180|             60|\n"
                + "|1240|             60|\n"
                + "|1300|             60|\n"
                + "|1360|             60|\n"
                + "|1420|             60|\n"
                + "|1480|             60|\n"
                + "|1540|             60|\n"
                + "+----+---------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+---------------------+\n"
                + "| key|first_value(us.d1.s1)|\n"
                + "+----+---------------------+\n"
                + "|1000|                 1000|\n"
                + "|1060|                 1060|\n"
                + "|1120|                 1120|\n"
                + "|1180|                 1180|\n"
                + "|1240|                 1240|\n"
                + "|1300|                 1300|\n"
                + "|1360|                 1360|\n"
                + "|1420|                 1420|\n"
                + "|1480|                 1480|\n"
                + "|1540|                 1540|\n"
                + "+----+---------------------+\n"
                + "Total line number = 10\n",
            "ResultSets:\n"
                + "+----+--------------------+\n"
                + "| key|last_value(us.d1.s1)|\n"
                + "+----+--------------------+\n"
                + "|1000|                1059|\n"
                + "|1060|                1119|\n"
                + "|1120|                1179|\n"
                + "|1180|                1239|\n"
                + "|1240|                1299|\n"
                + "|1300|                1359|\n"
                + "|1360|                1419|\n"
                + "|1420|                1479|\n"
                + "|1480|                1539|\n"
                + "|1540|                1599|\n"
                + "+----+--------------------+\n"
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
        "SELECT avg_s1 FROM (SELECT AVG(s1) AS avg_s1 FROM us.d1 OVER WINDOW (size 100 IN [1000, 1600))) WHERE avg_s1 > 1200;";
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
        "SELECT avg_s1 FROM (SELECT AVG(s1) AS avg_s1 FROM us.d1 WHERE us.d1.s1 < 1500 OVER WINDOW (size 100 IN [1000, 1600))) WHERE avg_s1 > 1200;";
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
        "SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 OVER WINDOW (size 10 IN [1000, 1100));";
    String expected =
        "ResultSets:\n"
            + "+----+------------+----------+------+------+\n"
            + "| key|window_start|window_end|avg_s1|sum_s2|\n"
            + "+----+------------+----------+------+------+\n"
            + "|1000|        1000|      1009|1004.5| 10055|\n"
            + "|1010|        1010|      1019|1014.5| 10155|\n"
            + "|1020|        1020|      1029|1024.5| 10255|\n"
            + "|1030|        1030|      1039|1034.5| 10355|\n"
            + "|1040|        1040|      1049|1044.5| 10455|\n"
            + "|1050|        1050|      1059|1054.5| 10555|\n"
            + "|1060|        1060|      1069|1064.5| 10655|\n"
            + "|1070|        1070|      1079|1074.5| 10755|\n"
            + "|1080|        1080|      1089|1084.5| 10855|\n"
            + "|1090|        1090|      1099|1094.5| 10955|\n"
            + "+----+------------+----------+------+------+\n"
            + "Total line number = 10\n";
    executor.executeAndCompare(statement, expected);

    statement =
        "SELECT avg_s1, sum_s2 "
            + "FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 "
            + "FROM us.d1 OVER WINDOW (size 10 IN [1000, 1100))) "
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
            + "FROM us.d1 OVER WINDOW (size 10 IN [1000, 1100))) "
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

    statement = "SELECT * FROM test.a INNER JOIN (SELECT a FROM test.b) ON test.a.a < test.b.a;";
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
        "SELECT * FROM test.a LEFT OUTER JOIN (SELECT a FROM test.b) ON test.a.a < test.b.a;";
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

    statement = "SELECT test.a.a, test.c.a FROM test.a INNER JOIN test.c ON test.a.d = test.c.d;";
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
        "SELECT test.a.a, test.c.a, (SELECT AVG(a) FROM test.b) FROM test.a INNER JOIN test.c ON test.a.d = test.c.d;";
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

    statement = "SELECT * FROM test.a WHERE d IN (SELECT d AS a FROM test.b ORDER BY key LIMIT 2);";
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

    statement =
        "SELECT * FROM test.a WHERE d = SOME (SELECT d AS a FROM test.b ORDER BY key LIMIT 2);";
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

    statement = "SELECT * FROM test.a WHERE a > (SELECT AVG(a) AS a FROM test.b);";
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
        "SELECT * FROM test.a WHERE (SELECT AVG(a) AS a FROM test.c) = (SELECT AVG(a) AS b FROM test.b);";
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
  public void testCTE() {
    String insert =
        "INSERT INTO bonus_jan(key, employee_id, name, outlet, position, region, bonus) VALUES "
            + "(0, 1, \"Max Black\", 123, \"manager\", \"South\", 2304), (1, 2, \"Jane Wolf\", 123, \"cashier\", \"South\", 1215), "
            + "(2, 3, \"Kate White\", 123, \"customer\", \"South\", 1545), (3, 4, \"Andrew Smart\", 123, \"customer\", \"South\", 1800), "
            + "(4, 5, \"John Ruder\", 105, \"manager\", \"South\", 2550), (5, 6, \"Sebastian Cornell\", 105, \"cashier\", \"South\", 1503), "
            + "(6, 7, \"Diana Johnson\", 105, \"customer\", \"North\", 2007), (7, 8, \"Sofia Blanc\", 224, \"manager\", \"North\", 2469), "
            + "(8, 9, \"Jack Spider\", 224, \"customer\", \"North\", 2100), (9, 10, \"Maria Le\", 224, \"cashier\", \"North\", 1335), "
            + "(10, 11, \"Anna Winfrey\", 211, \"manager\", \"North\", 2370), (11, 12, \"Marion Spencer\", 211, \"cashier\", \"North\", 1425);";
    executor.execute(insert);

    // test single CTE
    String statement =
        "WITH avg_position(position, average_bonus_for_position) AS (SELECT position, AVG(bonus) FROM bonus_jan GROUP BY position)\n"
            + "SELECT b.name, b.position, b.bonus, ap.average_bonus_for_position "
            + "FROM bonus_jan AS b "
            + "JOIN avg_position AS ap ON b.position = ap.position AND b.bonus > ap.average_bonus_for_position;";
    String expected =
        "ResultSets:\n"
            + "+-----------------+----------+-------+-----------------------------+\n"
            + "|           b.name|b.position|b.bonus|ap.average_bonus_for_position|\n"
            + "+-----------------+----------+-------+-----------------------------+\n"
            + "|       John Ruder|   manager|   2550|                      2423.25|\n"
            + "|Sebastian Cornell|   cashier|   1503|                       1369.5|\n"
            + "|    Diana Johnson|  customer|   2007|                       1863.0|\n"
            + "|      Sofia Blanc|   manager|   2469|                      2423.25|\n"
            + "|      Jack Spider|  customer|   2100|                       1863.0|\n"
            + "|   Marion Spencer|   cashier|   1425|                       1369.5|\n"
            + "+-----------------+----------+-------+-----------------------------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(statement, expected);

    // test multiple CTEs
    statement =
        "WITH avg_position(position, average_bonus_for_position) AS (SELECT position, AVG(bonus) FROM bonus_jan GROUP BY position),\n"
            + "avg_region(region, average_bonus_for_region) AS (SELECT region, AVG(bonus) FROM bonus_jan GROUP BY region)\n"
            + "SELECT b.name, b.position, b.region, b.bonus, ap.average_bonus_for_position, ar.average_bonus_for_region "
            + "FROM bonus_jan AS b "
            + "JOIN avg_position AS ap ON b.position = ap.position AND b.bonus > ap.average_bonus_for_position "
            + "JOIN avg_region AS ar ON b.region = ar.region AND b.bonus > ar.average_bonus_for_region;";
    expected =
        "ResultSets:\n"
            + "+-------------+----------+--------+-------+-----------------------------+---------------------------+\n"
            + "|       b.name|b.position|b.region|b.bonus|ap.average_bonus_for_position|ar.average_bonus_for_region|\n"
            + "+-------------+----------+--------+-------+-----------------------------+---------------------------+\n"
            + "|   John Ruder|   manager|   South|   2550|                      2423.25|                     1819.5|\n"
            + "|Diana Johnson|  customer|   North|   2007|                       1863.0|                     1951.0|\n"
            + "|  Sofia Blanc|   manager|   North|   2469|                      2423.25|                     1951.0|\n"
            + "|  Jack Spider|  customer|   North|   2100|                       1863.0|                     1951.0|\n"
            + "+-------------+----------+--------+-------+-----------------------------+---------------------------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(statement, expected);

    // test nested CTEs
    statement =
        "WITH avg_per_outlet(outlet, average_bonus_for_outlet) AS (SELECT outlet, AVG(bonus) FROM bonus_jan GROUP BY outlet),\n"
            + "min_bonus_outlet(min_avg_bonus_for_outlet) AS (SELECT MIN(average_bonus_for_outlet) FROM avg_per_outlet),\n"
            + "max_bonus_outlet(max_avg_bonus_for_outlet) AS (SELECT MAX(average_bonus_for_outlet) FROM avg_per_outlet)\n"
            + "SELECT ao.outlet, ao.average_bonus_for_outlet, min.min_avg_bonus_for_outlet, max.max_avg_bonus_for_outlet "
            + "FROM avg_per_outlet AS ao "
            + "CROSS JOIN min_bonus_outlet AS min "
            + "CROSS JOIN max_bonus_outlet AS max;";
    expected =
        "ResultSets:\n"
            + "+---------+---------------------------+----------------------------+----------------------------+\n"
            + "|ao.outlet|ao.average_bonus_for_outlet|min.min_avg_bonus_for_outlet|max.max_avg_bonus_for_outlet|\n"
            + "+---------+---------------------------+----------------------------+----------------------------+\n"
            + "|      211|                     1897.5|                      1716.0|                      2020.0|\n"
            + "|      105|                     2020.0|                      1716.0|                      2020.0|\n"
            + "|      123|                     1716.0|                      1716.0|                      2020.0|\n"
            + "|      224|                     1968.0|                      1716.0|                      2020.0|\n"
            + "+---------+---------------------------+----------------------------+----------------------------+\n"
            + "Total line number = 4\n";
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

    insert = "INSERT INTO prefix(key, suffix1) VALUES (0, \"b.a\"), (1, \"a.b\");";
    executor.execute(insert);

    insert = "INSERT INTO prefix(key, suffix2) VALUES (0, \"c.b\"), (2, \"a.a\");";
    executor.execute(insert);

    String statement = "SELECT * FROM prefix_test;";
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

    // test value2meta with multiple columns
    statement = "SELECT * FROM prefix;";
    expected =
        "ResultSets:\n"
            + "+---+--------------+--------------+\n"
            + "|key|prefix.suffix1|prefix.suffix2|\n"
            + "+---+--------------+--------------+\n"
            + "|  0|           b.a|           c.b|\n"
            + "|  1|           a.b|          null|\n"
            + "|  2|          null|           a.a|\n"
            + "+---+--------------+--------------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);

    statement = "SELECT VALUE2META(SELECT * FROM prefix) FROM test WHERE key < 4;";
    expected =
        "ResultSets:\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|key|test.b.a|test.c.b|test.a.b|test.a.a|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "|  1|   232.1|    true|     871|   apple|\n"
            + "|  2|   132.5|   false|     123|   peach|\n"
            + "|  3|   317.8|    true|     356|  banana|\n"
            + "+---+--------+--------+--------+--------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(statement, expected);
  }

  @Test
  public void testSelectFromShowColumns() {
    String insert =
        "INSERT INTO test(key, a, b, c, d) VALUES (0, 1, 1.5, true, \"aaa\"), (1, 2, 2.5, false, \"bbb\"), "
            + "(2, 3, 3.5, true, \"ccc\"), (3, 4, 4.5, false, \"ddd\"), (4, 5, 5.5, true, \"eee\");";
    executor.execute(insert);

    String query = "SELECT * FROM (SHOW COLUMNS test.*, us.*);";
    String expected =
        "ResultSets:\n"
            + "+--------+-------+\n"
            + "|    path|   type|\n"
            + "+--------+-------+\n"
            + "|  test.a|   LONG|\n"
            + "|  test.b| DOUBLE|\n"
            + "|  test.c|BOOLEAN|\n"
            + "|  test.d| BINARY|\n"
            + "|us.d1.s1|   LONG|\n"
            + "|us.d1.s2|   LONG|\n"
            + "|us.d1.s3| BINARY|\n"
            + "|us.d1.s4| DOUBLE|\n"
            + "+--------+-------+\n"
            + "Total line number = 8\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT path FROM (SHOW COLUMNS us.*);";
    expected =
        "ResultSets:\n"
            + "+--------+\n"
            + "|    path|\n"
            + "+--------+\n"
            + "|us.d1.s1|\n"
            + "|us.d1.s2|\n"
            + "|us.d1.s3|\n"
            + "|us.d1.s4|\n"
            + "+--------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT path FROM (SHOW COLUMNS test.*, us.* LIMIT 3);";
    expected =
        "ResultSets:\n"
            + "+------+\n"
            + "|  path|\n"
            + "+------+\n"
            + "|test.a|\n"
            + "|test.b|\n"
            + "|test.c|\n"
            + "+------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT path FROM (SHOW COLUMNS test.*, us.*) LIMIT 3;";
    expected =
        "ResultSets:\n"
            + "+------+\n"
            + "|  path|\n"
            + "+------+\n"
            + "|test.a|\n"
            + "|test.b|\n"
            + "|test.c|\n"
            + "+------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(query, expected);

    query =
        "SELECT path FROM (SHOW COLUMNS us.*) WHERE path LIKE \".*.s3\" OR path LIKE \".*.s4\";";
    expected =
        "ResultSets:\n"
            + "+--------+\n"
            + "|    path|\n"
            + "+--------+\n"
            + "|us.d1.s3|\n"
            + "|us.d1.s4|\n"
            + "+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT path AS p FROM (SHOW COLUMNS us.*) WHERE type = \"LONG\";";
    expected =
        "ResultSets:\n"
            + "+--------+\n"
            + "|       p|\n"
            + "+--------+\n"
            + "|us.d1.s1|\n"
            + "|us.d1.s2|\n"
            + "+--------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT type AS t FROM (SHOW COLUMNS test.*) GROUP BY type ORDER BY type;";
    expected =
        "ResultSets:\n"
            + "+-------+\n"
            + "|      t|\n"
            + "+-------+\n"
            + "| BINARY|\n"
            + "|BOOLEAN|\n"
            + "| DOUBLE|\n"
            + "|   LONG|\n"
            + "+-------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query =
        "SELECT * FROM (SHOW COLUMNS test.*) AS test JOIN (SHOW COLUMNS us.*) AS us ON test.type = us.type;";
    expected =
        "ResultSets:\n"
            + "+---------+---------+--------+-------+\n"
            + "|test.path|test.type| us.path|us.type|\n"
            + "+---------+---------+--------+-------+\n"
            + "|   test.a|     LONG|us.d1.s1|   LONG|\n"
            + "|   test.a|     LONG|us.d1.s2|   LONG|\n"
            + "|   test.b|   DOUBLE|us.d1.s4| DOUBLE|\n"
            + "|   test.d|   BINARY|us.d1.s3| BINARY|\n"
            + "+---------+---------+--------+-------+\n"
            + "Total line number = 4\n";
    executor.executeAndCompare(query, expected);

    query =
        "SELECT * FROM (SHOW COLUMNS test.*) AS test LEFT JOIN (SHOW COLUMNS us.*) AS us ON test.type = us.type;";
    expected =
        "ResultSets:\n"
            + "+---------+---------+--------+-------+\n"
            + "|test.path|test.type| us.path|us.type|\n"
            + "+---------+---------+--------+-------+\n"
            + "|   test.a|     LONG|us.d1.s1|   LONG|\n"
            + "|   test.a|     LONG|us.d1.s2|   LONG|\n"
            + "|   test.b|   DOUBLE|us.d1.s4| DOUBLE|\n"
            + "|   test.d|   BINARY|us.d1.s3| BINARY|\n"
            + "|   test.c|  BOOLEAN|    null|   null|\n"
            + "+---------+---------+--------+-------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT * FROM test WHERE EXISTS (SELECT * FROM (SHOW COLUMNS us.*));";
    expected =
        "ResultSets:\n"
            + "+---+------+------+------+------+\n"
            + "|key|test.a|test.b|test.c|test.d|\n"
            + "+---+------+------+------+------+\n"
            + "|  0|     1|   1.5|  true|   aaa|\n"
            + "|  1|     2|   2.5| false|   bbb|\n"
            + "|  2|     3|   3.5|  true|   ccc|\n"
            + "|  3|     4|   4.5| false|   ddd|\n"
            + "|  4|     5|   5.5|  true|   eee|\n"
            + "+---+------+------+------+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query =
        "SELECT * FROM test WHERE EXISTS (SELECT * FROM (SHOW COLUMNS us.*) WHERE type = \"BOOLEAN\");";
    expected =
        "ResultSets:\n"
            + "+---+------+------+------+------+\n"
            + "|key|test.a|test.b|test.c|test.d|\n"
            + "+---+------+------+------+------+\n"
            + "+---+------+------+------+------+\n"
            + "Empty set.\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testMixedValueToMetaAndShowColumns() {
    String insert =
        "INSERT INTO test(key, a, b, c, d) VALUES (0, 1, 1.5, true, \"aaa\"), (1, 2, 2.5, false, \"bbb\"), "
            + "(2, 3, 3.5, true, \"ccc\"), (3, 4, 4.5, false, \"ddd\"), (4, 5, 5.5, true, \"eee\");";
    executor.execute(insert);

    String query =
        "SELECT path FROM (SHOW COLUMNS test.*) WHERE type = \"LONG\" OR type = \"DOUBLE\" ORDER BY path;";
    String expected =
        "ResultSets:\n"
            + "+------+\n"
            + "|  path|\n"
            + "+------+\n"
            + "|test.a|\n"
            + "|test.b|\n"
            + "+------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT test.a, test.b FROM (SELECT * FROM test);";
    expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  0|     1|   1.5|\n"
            + "|  1|     2|   2.5|\n"
            + "|  2|     3|   3.5|\n"
            + "|  3|     4|   4.5|\n"
            + "|  4|     5|   5.5|\n"
            + "+---+------+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query =
        "SELECT VALUE2META(SELECT path FROM (SHOW COLUMNS test.*) WHERE type = \"LONG\" OR type = \"DOUBLE\" ORDER BY path) FROM (SELECT * FROM test);";
    expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  0|     1|   1.5|\n"
            + "|  1|     2|   2.5|\n"
            + "|  2|     3|   3.5|\n"
            + "|  3|     4|   4.5|\n"
            + "|  4|     5|   5.5|\n"
            + "+---+------+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);
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
        "INSERT INTO us.d4(key, s1, s2) VALUES "
            + "(SELECT avg_s1, sum_s2 from "
            + "(SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 OVER WINDOW (size 10 IN [1000, 1100))));";
    executor.execute(insert);

    query = "SELECT s1, s2 FROM us.d4;";
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
            + "FROM us.d1 OVER WINDOW (size 10 IN [1000, 1100))) "
            + "WHERE avg_s1 > 1020 AND sum_s2 < 10800);";
    executor.execute(insert);

    query = "SELECT s1, s2 FROM us.d5;";
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
            + "FROM us.d1 OVER WINDOW (size 10 IN [1000, 1100))) "
            + "WHERE avg_s1 > 1020 AND sum_s2 < 10800));";
    executor.execute(insert);

    query = "SELECT s1, s2 FROM us.d6;";
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
  public void testInsertWithSubQueryWithNull() {
    String insert =
        "INSERT INTO test(key, a, b, c) VALUES (0, 0, 0.5, \"aaa\"), (1, 1, 1.5, \"bbb\"), "
            + "(2, null, 2.5, \"ccc\"), (3, null, 3.5, \"ddd\"), (4, null, 4.5, null), (5, null, 5.5, null);";
    executor.execute(insert);

    String query = "SELECT * FROM test;";
    String expected =
        "ResultSets:\n"
            + "+---+------+------+------+\n"
            + "|key|test.a|test.b|test.c|\n"
            + "+---+------+------+------+\n"
            + "|  0|     0|   0.5|   aaa|\n"
            + "|  1|     1|   1.5|   bbb|\n"
            + "|  2|  null|   2.5|   ccc|\n"
            + "|  3|  null|   3.5|   ddd|\n"
            + "|  4|  null|   4.5|  null|\n"
            + "|  5|  null|   5.5|  null|\n"
            + "+---+------+------+------+\n"
            + "Total line number = 6\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT * FROM t;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executor.executeAndCompare(query, expected);

    String insertFromSelect = "INSERT INTO t(key, a, b, c) VALUES (SELECT * FROM test);";
    executor.execute(insertFromSelect);

    query = "SELECT * FROM t;";
    expected =
        "ResultSets:\n"
            + "+---+----+---+----+\n"
            + "|key| t.a|t.b| t.c|\n"
            + "+---+----+---+----+\n"
            + "|  0|   0|0.5| aaa|\n"
            + "|  1|   1|1.5| bbb|\n"
            + "|  2|null|2.5| ccc|\n"
            + "|  3|null|3.5| ddd|\n"
            + "|  4|null|4.5|null|\n"
            + "|  5|null|5.5|null|\n"
            + "+---+----+---+----+\n"
            + "Total line number = 6\n";
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
        "INSERT INTO `114514`(key, `1919810`) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
    executor.execute(insert);

    String query = "SELECT `1919810` FROM `114514`;";
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

    query = "SELECT `1919810` * 2 FROM `114514`;";
    expected =
        "ResultSets:\n"
            + "+---+------------------+\n"
            + "|key|114514.1919810 × 2|\n"
            + "+---+------------------+\n"
            + "|  1|                 2|\n"
            + "|  2|                 4|\n"
            + "|  3|                 6|\n"
            + "|  4|                 8|\n"
            + "|  5|                10|\n"
            + "+---+------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT 2 * `1919810` FROM `114514`;";
    expected =
        "ResultSets:\n"
            + "+---+------------------+\n"
            + "|key|2 × 114514.1919810|\n"
            + "+---+------------------+\n"
            + "|  1|                 2|\n"
            + "|  2|                 4|\n"
            + "|  3|                 6|\n"
            + "|  4|                 8|\n"
            + "|  5|                10|\n"
            + "+---+------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT `1919810` FROM `114514` WHERE `1919810` < 3;";
    expected =
        "ResultSets:\n"
            + "+---+--------------+\n"
            + "|key|114514.1919810|\n"
            + "+---+--------------+\n"
            + "|  1|             1|\n"
            + "|  2|             2|\n"
            + "+---+--------------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);

    query = "SELECT `1919810` FROM `114514` WHERE 3 < `1919810`;";
    expected =
        "ResultSets:\n"
            + "+---+--------------+\n"
            + "|key|114514.1919810|\n"
            + "+---+--------------+\n"
            + "|  4|             4|\n"
            + "|  5|             5|\n"
            + "+---+--------------+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expected);
  }

  @Test
  public void testSpecialCharacterPath() {
    // filesystem does not support special character path on windows
    if (!isSupportSpecialCharacterPath) {
      return;
    }

    boolean isTestingFilesystemOnWin = isOnWin && runningEngine.equalsIgnoreCase("filesystem");

    if (!isTestingFilesystemOnWin) {
      // IGinX SQL 路径中支持的合法字符
      String insert =
          "INSERT INTO _:@#$~^{}(key, _:@#$~^{}, _:@#$~\\^) VALUES (1, 1, 2), (2, 2, 3), (3, 3, 4), (4, 4, 4), (5, 5, 5);";
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

      query = "SELECT _:@#$~^{} FROM _:@#$~^{} WHERE _:@#$~^{} >= 2 AND _:@#$~^{} <= 4;";
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

      query = "SELECT _:@#$~^{}, _:@#$~\\^ FROM _:@#$~^{} WHERE _:@#$~^{} < _:@#$~\\^;";
      expected =
          "ResultSets:\n"
              + "+---+-------------------+------------------+\n"
              + "|key|_:@#$~^{}._:@#$~^{}|_:@#$~^{}._:@#$~\\^|\n"
              + "+---+-------------------+------------------+\n"
              + "|  1|                  1|                 2|\n"
              + "|  2|                  2|                 3|\n"
              + "|  3|                  3|                 4|\n"
              + "+---+-------------------+------------------+\n"
              + "Total line number = 3\n";
      executor.executeAndCompare(query, expected);
    } else {
      // :, \\ can't be used in filesystem in windows
      String insert =
          "INSERT INTO _@#$~^{}(key, _@#$~^{}, _@#$~^) VALUES (1, 1, 2), (2, 2, 3), (3, 3, 4), (4, 4, 4), (5, 5, 5);";
      executor.execute(insert);

      String query = "SELECT _@#$~^{} FROM _@#$~^{};";
      String expected =
          "ResultSets:\n"
              + "+---+-----------------+\n"
              + "|key|_@#$~^{}._@#$~^{}|\n"
              + "+---+-----------------+\n"
              + "|  1|                1|\n"
              + "|  2|                2|\n"
              + "|  3|                3|\n"
              + "|  4|                4|\n"
              + "|  5|                5|\n"
              + "+---+-----------------+\n"
              + "Total line number = 5\n";
      executor.executeAndCompare(query, expected);

      query = "SELECT _@#$~^{} FROM _@#$~^{} WHERE _@#$~^{} >= 2 AND _@#$~^{} <= 4;";
      expected =
          "ResultSets:\n"
              + "+---+-----------------+\n"
              + "|key|_@#$~^{}._@#$~^{}|\n"
              + "+---+-----------------+\n"
              + "|  2|                2|\n"
              + "|  3|                3|\n"
              + "|  4|                4|\n"
              + "+---+-----------------+\n"
              + "Total line number = 3\n";
      executor.executeAndCompare(query, expected);

      query = "SELECT _@#$~^{}, _@#$~^ FROM _@#$~^{} WHERE _@#$~^{} < _@#$~^;";
      expected =
          "ResultSets:\n"
              + "+---+-----------------+---------------+\n"
              + "|key|_@#$~^{}._@#$~^{}|_@#$~^{}._@#$~^|\n"
              + "+---+-----------------+---------------+\n"
              + "|  1|                1|              2|\n"
              + "|  2|                2|              3|\n"
              + "|  3|                3|              4|\n"
              + "+---+-----------------+---------------+\n"
              + "Total line number = 3\n";
      executor.executeAndCompare(query, expected);
    }
  }

  @Test
  public void testMixSpecialPath() {
    // filesystem does not support special character path on windows
    if (!isSupportChinesePath || !isSupportNumericalPath || !isSupportSpecialCharacterPath) {
      return;
    }

    boolean isTestingFilesystemOnWin = isOnWin && runningEngine.equalsIgnoreCase("filesystem");

    if (!isTestingFilesystemOnWin) {
      // mix path
      String insert =
          "INSERT INTO 测试.前缀.`114514`(key, `1919810`._:@#$.后缀) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
      executor.execute(insert);

      String query = "SELECT `1919810`._:@#$.后缀 FROM 测试.前缀.`114514`;";
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
    } else {
      // :, \\ can't be used in filesystem in windows
      // mix path
      String insert =
          "INSERT INTO 测试.前缀.`114514`(key, `1919810`._@#$.后缀) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
      executor.execute(insert);

      String query = "SELECT `1919810`._@#$.后缀 FROM 测试.前缀.`114514`;";
      String expected =
          "ResultSets:\n"
              + "+---+----------------------------+\n"
              + "|key|测试.前缀.114514.1919810._@#$.后缀|\n"
              + "+---+----------------------------+\n"
              + "|  1|                           1|\n"
              + "|  2|                           2|\n"
              + "|  3|                           3|\n"
              + "|  4|                           4|\n"
              + "|  5|                           5|\n"
              + "+---+----------------------------+\n"
              + "Total line number = 5\n";
      executor.executeAndCompare(query, expected);
    }
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

    errClause = "SELECT s1 FROM us.d1 OVER WINDOW (size 100 IN (0, 1000));";
    executor.executeAndCompareErrMsg(
        errClause, "Downsample clause cannot be used without aggregate function.");

    errClause = "SELECT last(s1), max(s2) FROM us.d1;";
    executor.executeAndCompareErrMsg(
        errClause, "SetToSet/SetToRow/RowToRow functions can not be mixed in aggregate query.");

    errClause = "SELECT s1 FROM us.d1 OVER WINDOW (size 100 IN (100, 10));";
    executor.executeAndCompareErrMsg(
        errClause, "start key should be smaller than end key in key interval.");

    errClause = "SELECT last(s1) FROM us.d1 GROUP BY s2;";
    executor.executeAndCompareErrMsg(
        errClause, "Group by can not use SetToSet and RowToRow functions.");

    errClause = "select * from test.a join test.b where a > 0;";
    executor.executeAndCompareErrMsg(errClause, "Unexpected paths' name: [a].");

    errClause = "SELECT 1 * 2 FROM test;";
    executor.executeAndCompareErrMsg(
        errClause, "SELECT constant arithmetic expression isn't supported yet.");

    errClause = "select * from (show columns a.*), (show columns b.*);";
    executor.executeAndCompareErrMsg(
        errClause, "As clause is expected when multiple ShowColumns are joined together.");
  }

  @Test
  public void testExplain() {
    if (isScaling) return;
    String explain = "explain select max(s2), min(s1) from us.d1;";
    String expected =
        "ResultSets:\n"
            + "+-----------------+-------------+--------------------------------------------------------------------------------------------------+\n"
            + "|     Logical Tree|Operator Type|                                                                                     Operator Info|\n"
            + "+-----------------+-------------+--------------------------------------------------------------------------------------------------+\n"
            + "|Reorder          |      Reorder|                                                                Order: max(us.d1.s2),min(us.d1.s1)|\n"
            + "|  +--SetTransform| SetTransform|FuncList(Name, FuncType): (min, System), (max, System), MappingType: SetMapping, isDistinct: false|\n"
            + "|    +--Project   |      Project|                                            Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000000|\n"
            + "+-----------------+-------------+--------------------------------------------------------------------------------------------------+\n"
            + "Total line number = 3\n";
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
    LOGGER.info(executor.execute(explain));

    explain = "explain physical select s1 from us.d1 where s1 > 10 and s1 < 100;";
    LOGGER.info(executor.execute(explain));
  }

  @Test
  public void testDeleteColumns() {
    if (!isAbleToDelete || isScaling) {
      return;
    }
    String showColumns = "SHOW COLUMNS us.*;";
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
    executor.executeAndCompare(showColumns, expected);

    String deleteTimeSeries = "DELETE COLUMNS us.d1.s4;";
    executor.execute(deleteTimeSeries);

    showColumns = "SHOW COLUMNS us.*;";
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
    executor.executeAndCompare(showColumns, expected);

    String showColumnsData = "SELECT s4 FROM us.d1;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executor.executeAndCompare(showColumnsData, expected);

    deleteTimeSeries = "DELETE COLUMNS us.*;";
    executor.execute(deleteTimeSeries);

    showColumns = "SHOW COLUMNS us.*;";
    expected =
        "Columns:\n"
            + "+----+--------+\n"
            + "|Path|DataType|\n"
            + "+----+--------+\n"
            + "+----+--------+\n"
            + "Empty set.\n";
    executor.executeAndCompare(showColumns, expected);

    showColumnsData = "SELECT * FROM *;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executor.executeAndCompare(showColumnsData, expected);

    String countPoints = "COUNT POINTS;";
    expected = "Points num: 0\n";
    executor.executeAndCompare(countPoints, expected);
  }

  @Test
  public void testClearData() throws SessionException {
    if (!isAbleToClearData || isScaling) return;
    clearData();

    String countPoints = "COUNT POINTS;";
    String expected = "Points num: 0\n";
    executor.executeAndCompare(countPoints, expected);

    String showColumns = "SELECT * FROM *;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executor.executeAndCompare(showColumns, expected);
  }

  @Test
  public void testConcurrentDeleteSinglePath() {
    if (!isAbleToDelete || isScaling) {
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

    String query = "SELECT s1 FROM us.d1 WHERE key > 995 AND key < 1255;";
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
    if (!isAbleToDelete || isScaling) {
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
    if (!isAbleToDelete || isScaling) {
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
    if (!isAbleToDelete || isScaling) {
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
    if (isScaling) {
      return;
    }
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
    if (isScaling) {
      return;
    }
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
                "SHOW COLUMNS;",
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
        "insert into test1(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
    executor.execute(insert);
    insert =
        "insert into test2(key, a, b, c, d) values (1, 3, 2, 3.1, \"val1\"), (2, 1, 3, 2.1, \"val2\"), (3, 2, 2, 1.1, \"val5\"), (4, 3, 2, 2.1, \"val2\"), (5, 1, 2, 3.1, \"val1\"), (6, 2, 2, 5.1, \"val3\");";
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
                "SELECT max(s1), max(s4) FROM us.d1 WHERE key > 300 AND s1 <= 600 OVER WINDOW (size 100 IN (0, 1000) SLIDE 50);",
                "ResultSets:\n"
                    + "+---+------------+----------+-------------+-------------+\n"
                    + "|key|window_start|window_end|max(us.d1.s1)|max(us.d1.s4)|\n"
                    + "+---+------------+----------+-------------+-------------+\n"
                    + "|251|         251|       350|          350|        350.1|\n"
                    + "|301|         301|       400|          400|        400.1|\n"
                    + "|351|         351|       450|          450|        450.1|\n"
                    + "|401|         401|       500|          500|        500.1|\n"
                    + "|451|         451|       550|          550|        550.1|\n"
                    + "|501|         501|       600|          600|        600.1|\n"
                    + "|551|         551|       650|          600|        600.1|\n"
                    + "+---+------------+----------+-------------+-------------+\n"
                    + "Total line number = 7\n"),
            new Pair<>(
                "select avg(test1.a), test2.d from test1 join test2 on test1.a = test2.a group by test2.d;",
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
                "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d;",
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
                "select avg(test1.a), max(test1.c), test2.d from test1 join test2 on test1.a = test2.a group by test2.d having max(test1.c) > 3.5;",
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
  public void testShowConfig() {
    String statement = "show config \"memoryTaskThreadPoolSize\";";
    String expected =
        "Config Info:\n"
            + "+------------------------+-----------+\n"
            + "|              ConfigName|ConfigValue|\n"
            + "+------------------------+-----------+\n"
            + "|memoryTaskThreadPoolSize|        200|\n"
            + "+------------------------+-----------+\n";
    executor.executeAndCompare(statement, expected);

    statement = "show config \"enableEnvParameter\";";
    expected =
        "Config Info:\n"
            + "+------------------+-----------+\n"
            + "|        ConfigName|ConfigValue|\n"
            + "+------------------+-----------+\n"
            + "|enableEnvParameter|      false|\n"
            + "+------------------+-----------+\n";
    executor.executeAndCompare(statement, expected);

    statement = "show config;";
    Map<String, String> configs = executor.getSessionExecuteSqlResult(statement).getConfigs();
    Set<String> expectedConfigNames =
        new HashSet<>(
            Arrays.asList(
                "cachedTimeseriesProb",
                "parallelFilterThreshold",
                "memoryTaskThreadPoolSize",
                "statisticsCollectorClassName",
                "systemMemoryThreshold",
                "streamParallelGroupByWorkerNum",
                "password",
                "constraintChecker",
                "mqttHost",
                "fragmentCompactionReadRatioThreshold",
                "migrationPolicyClassName",
                "metaStorage",
                "tagPrefix",
                "ip",
                "enableRestService",
                "parallelGroupByPoolNum",
                "enablePushDown",
                "loadBalanceCheckInterval",
                "parallelApplyFuncGroupsThreshold",
                "timePrecision",
                "useStreamExecutor",
                "parallelGroupByRowsThreshold",
                "enableMonitor",
                "databaseClassNames",
                "mqttPort",
                "clients",
                "defaultUDFDir",
                "ruleBasedOptimizer",
                "instancesNumPerClient",
                "physicalOptimizer",
                "maxAsyncRetryTimes",
                "enableInstantCompaction",
                "maxCachedPhysicalTaskPerStorage",
                "restIp",
                "physicalTaskThreadPoolSizePerStorage",
                "disorderMargin",
                "enableEnvParameter",
                "syncExecuteThreadPool",
                "policyClassName",
                "fragmentCacheThreshold",
                "fragmentPerEngine",
                "enableStorageGroupValueLimit",
                "zookeeperConnectionString",
                "minThriftWorkerThreadNum",
                "udfList",
                "migrationBatchSize",
                "maxTimeseriesLoadBalanceThreshold",
                "mqttMaxMessageSize",
                "reshardFragmentTimeMargin",
                "storageGroupValueLimit",
                "replicaNum",
                "enableMetaCacheControl",
                "fragmentCompactionReadThreshold",
                "storageEngineList",
                "batchSizeImportCsv",
                "restPort",
                "asyncRestThreadPool",
                "fragmentCompactionWriteThreshold",
                "expectedStorageUnitNum",
                "maxThriftWrokerThreadNum",
                "isUTTestEnv",
                "tagSuffix",
                "port",
                "asyncExecuteThreadPool",
                "needInitBasicUDFFunctions",
                "retryWait",
                "reAllocatePeriod",
                "pythonCMD",
                "historicalPrefixList",
                "transformMaxRetryTimes",
                "transformTaskThreadPoolSize",
                "enableFragmentCompaction",
                "maxReshardFragmentsNum",
                "statisticsLogInterval",
                "enableMemoryControl",
                "retryCount",
                "tagNameAnnotation",
                "etcdEndpoints",
                "mqttPayloadFormatter",
                "queryOptimizer",
                "systemCpuThreshold",
                "enableMQTT",
                "mqttHandlerPoolSize",
                "heapMemoryThreshold",
                "systemResourceMetrics",
                "maxTimeseriesLength",
                "batchSize",
                "parallelGroupByPoolSize",
                "username",
                "enableEmailNotification",
                "mailSmtpHost",
                "mailSmtpPort",
                "mailSmtpUser",
                "mailSmtpPassword",
                "mailSender",
                "mailRecipient"));

    assertEquals(expectedConfigNames, configs.keySet());
  }

  @Test
  public void testModifyRules() {
    String statement, expected;
    statement = "show rules;";

    String ruleBasedOptimizer = executor.execute("SHOW CONFIG \"ruleBasedOptimizer\";");
    LOGGER.info("testModifyRules: {}", ruleBasedOptimizer);
    // 2种情况不测试Config设置Rule的效果：
    // 1. 本地环境下FragmentPruningByFilterRule默认是开启的，不测试
    // 2. SessionPool测试在Session测试后，此时FragmentPruningByFilterRule已经被开启，不测试
    if (ruleBasedOptimizer.contains("FragmentPruningByFilterRule=on") || isForSessionPool) {
      expected = "FragmentPruningByFilterRule|    ON";

    } else {
      expected = "FragmentPruningByFilterRule|   OFF";
    }

    assertTrue(executor.execute(statement).contains(expected));

    statement = "set rules FragmentPruningByFilterRule=on;";
    executor.execute(statement);

    statement = "show rules;";
    expected = "FragmentPruningByFilterRule|    ON";
    assertTrue(executor.execute(statement).contains(expected));

    statement = "set rules FragmentPruningByFilterRule=off, NotFilterRemoveRule=off;";
    executor.execute(statement);

    statement = "show rules;";
    String expected1 = "NotFilterRemoveRule|   OFF";
    String expected2 = "FragmentPruningByFilterRule|   OFF";
    String result = executor.execute(statement);
    assertTrue(result.contains(expected1) && result.contains(expected2));

    statement = "set rules FragmentPruningByFilterRule=on, NotFilterRemoveRule=on;";
    executor.execute(statement);

    statement = "show rules;";
    expected1 = "NotFilterRemoveRule|    ON";
    expected2 = "FragmentPruningByFilterRule|    ON";
    result = executor.execute(statement);
    assertTrue(result.contains(expected1) && result.contains(expected2));
  }

  @Test
  public void testFilterPushDownExplain() {
    // 临时修改
    if (!isFilterPushDown) {
      LOGGER.info(
          "Skip SQLSessionIT.testFilterPushDownExplain because filter_push_down optimizer is not open");
      return;
    }

    String insert =
        "INSERT INTO us.d2(key, c) VALUES (1, \"asdas\"), (2, \"sadaa\"), (3, \"sadada\"), (4, \"asdad\"), (5, \"deadsa\"), (6, \"dasda\"), (7, \"asdsad\"), (8, \"frgsa\"), (9, \"asdad\");";
    executor.execute(insert);
    insert =
        "INSERT INTO us.d3(key, s1) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9);";
    executor.execute(insert);

    String closeRule = "SET RULES FragmentPruningByPatternRule=OFF, ColumnPruningRule=OFF;";
    executor.execute(closeRule);

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
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|            Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                 |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project            |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select           |       Select|                   Filter: (us.d1.s1 < 4)|\n"
                    + "|      +--Join           |         Join|                              JoinBy: key|\n"
                    + "|        +--Select       |       Select|                   Filter: (us.d1.s1 < 4)|\n"
                    + "|          +--Join       |         Join|                              JoinBy: key|\n"
                    + "|            +--Select   |       Select|                   Filter: (us.d1.s1 < 4)|\n"
                    + "|              +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|            +--Project  |      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project      |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < 5 and d1.s2 > 2;",
                "ResultSets:\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|            Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                 |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project            |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select           |       Select|   Filter: (us.d1.s1 < 5 && us.d1.s2 > 2)|\n"
                    + "|      +--Join           |         Join|                              JoinBy: key|\n"
                    + "|        +--Select       |       Select|   Filter: (us.d1.s1 < 5 && us.d1.s2 > 2)|\n"
                    + "|          +--Join       |         Join|                              JoinBy: key|\n"
                    + "|            +--Select   |       Select|   Filter: (us.d1.s1 < 5 && us.d1.s2 > 2)|\n"
                    + "|              +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|            +--Project  |      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project      |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < 6 and d2.s1 > 3;",
                "ResultSets:\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|            Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                 |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project            |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select           |       Select|   Filter: (us.d1.s1 < 6 && us.d2.s1 > 3)|\n"
                    + "|      +--Join           |         Join|                              JoinBy: key|\n"
                    + "|        +--Select       |       Select|   Filter: (us.d1.s1 < 6 && us.d2.s1 > 3)|\n"
                    + "|          +--Join       |         Join|                              JoinBy: key|\n"
                    + "|            +--Select   |       Select|                   Filter: (us.d1.s1 < 6)|\n"
                    + "|              +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|            +--Select   |       Select|                   Filter: (us.d2.s1 > 3)|\n"
                    + "|              +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project      |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 11\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < 6 or d2.s1 < 7;\n",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select| Filter: ((us.d1.s1 < 6 || us.d2.s1 < 7))|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Select     |       Select| Filter: ((us.d1.s1 < 6 || us.d2.s1 < 7))|\n"
                    + "|          +--Join     |         Join|                              JoinBy: key|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 9\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d2.c like \"[a|s]\";",
                "ResultSets:\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|            Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                 |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project            |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select           |       Select|           Filter: (us.d2.c like \"[a|s]\")|\n"
                    + "|      +--Join           |         Join|                              JoinBy: key|\n"
                    + "|        +--Select       |       Select|           Filter: (us.d2.c like \"[a|s]\")|\n"
                    + "|          +--Join       |         Join|                              JoinBy: key|\n"
                    + "|            +--Project  |      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|            +--Select   |       Select|           Filter: (us.d2.c like \"[a|s]\")|\n"
                    + "|              +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project      |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE key < 4;\n",
                "ResultSets:\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder             |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project        |      Project|                           Patterns: us.*|\n"
                    + "|    +--Join         |         Join|                              JoinBy: key|\n"
                    + "|      +--Join       |         Join|                              JoinBy: key|\n"
                    + "|        +--Select   |       Select|                        Filter: (key < 4)|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|        +--Select   |       Select|                        Filter: (key < 4)|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|      +--Select     |       Select|                        Filter: (key < 4)|\n"
                    + "|        +--Project  |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE key < 5 and key > 1;\n",
                "ResultSets:\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder             |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project        |      Project|                           Patterns: us.*|\n"
                    + "|    +--Join         |         Join|                              JoinBy: key|\n"
                    + "|      +--Join       |         Join|                              JoinBy: key|\n"
                    + "|        +--Select   |       Select|             Filter: (key < 5 && key > 1)|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|        +--Select   |       Select|             Filter: (key < 5 && key > 1)|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|      +--Select     |       Select|             Filter: (key < 5 && key > 1)|\n"
                    + "|        +--Project  |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE key < 5 or key > 1003;",
                "ResultSets:\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder             |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project        |      Project|                           Patterns: us.*|\n"
                    + "|    +--Join         |         Join|                              JoinBy: key|\n"
                    + "|      +--Join       |         Join|                              JoinBy: key|\n"
                    + "|        +--Select   |       Select|        Filter: ((key < 5 || key > 1003))|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|        +--Select   |       Select|        Filter: ((key < 5 || key > 1003))|\n"
                    + "|          +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|      +--Select     |       Select|        Filter: ((key < 5 || key > 1003))|\n"
                    + "|        +--Project  |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+--------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < d1.s2;\n",
                "ResultSets:\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|            Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                 |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project            |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select           |       Select|            Filter: (us.d1.s1 < us.d1.s2)|\n"
                    + "|      +--Join           |         Join|                              JoinBy: key|\n"
                    + "|        +--Select       |       Select|            Filter: (us.d1.s1 < us.d1.s2)|\n"
                    + "|          +--Join       |         Join|                              JoinBy: key|\n"
                    + "|            +--Select   |       Select|            Filter: (us.d1.s1 < us.d1.s2)|\n"
                    + "|              +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|            +--Project  |      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project      |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us WHERE d1.s1 < d2.s1;\n",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder               |      Reorder|                              Order: us.*|\n"
                    + "|  +--Project          |      Project|                           Patterns: us.*|\n"
                    + "|    +--Select         |       Select|            Filter: (us.d1.s1 < us.d2.s1)|\n"
                    + "|      +--Join         |         Join|                              JoinBy: key|\n"
                    + "|        +--Select     |       Select|            Filter: (us.d1.s1 < us.d2.s1)|\n"
                    + "|          +--Join     |         Join|                              JoinBy: key|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|            +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|        +--Project    |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 9\n"),
            new Pair<>(
                "explain SELECT * FROM (SELECT * FROM us WHERE us.d1.s1 < 5) WHERE us.d1.s2 < 5;\n",
                "ResultSets:\n"
                    + "+----------------------------+-------------+-----------------------------------------+\n"
                    + "|                Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                     |      Reorder|                                 Order: *|\n"
                    + "|  +--Project                |      Project|                              Patterns: *|\n"
                    + "|    +--Reorder              |      Reorder|                              Order: us.*|\n"
                    + "|      +--Project            |      Project|                           Patterns: us.*|\n"
                    + "|        +--Select           |       Select|   Filter: (us.d1.s2 < 5 && us.d1.s1 < 5)|\n"
                    + "|          +--Join           |         Join|                              JoinBy: key|\n"
                    + "|            +--Select       |       Select|   Filter: (us.d1.s2 < 5 && us.d1.s1 < 5)|\n"
                    + "|              +--Join       |         Join|                              JoinBy: key|\n"
                    + "|                +--Select   |       Select|   Filter: (us.d1.s2 < 5 && us.d1.s1 < 5)|\n"
                    + "|                  +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|                +--Project  |      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|            +--Project      |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 12\n"),
            new Pair<>(
                "explain SELECT * FROM (SELECT * FROM us WHERE us.d1.s1 < 5) WHERE us.d2.s1 < 10;",
                "ResultSets:\n"
                    + "+----------------------------+-------------+-----------------------------------------+\n"
                    + "|                Logical Tree|Operator Type|                            Operator Info|\n"
                    + "+----------------------------+-------------+-----------------------------------------+\n"
                    + "|Reorder                     |      Reorder|                                 Order: *|\n"
                    + "|  +--Project                |      Project|                              Patterns: *|\n"
                    + "|    +--Reorder              |      Reorder|                              Order: us.*|\n"
                    + "|      +--Project            |      Project|                           Patterns: us.*|\n"
                    + "|        +--Select           |       Select|  Filter: (us.d2.s1 < 10 && us.d1.s1 < 5)|\n"
                    + "|          +--Join           |         Join|                              JoinBy: key|\n"
                    + "|            +--Select       |       Select|  Filter: (us.d2.s1 < 10 && us.d1.s1 < 5)|\n"
                    + "|              +--Join       |         Join|                              JoinBy: key|\n"
                    + "|                +--Select   |       Select|                   Filter: (us.d1.s1 < 5)|\n"
                    + "|                  +--Project|      Project|Patterns: us.*, Target DU: unit0000000000|\n"
                    + "|                +--Select   |       Select|                  Filter: (us.d2.s1 < 10)|\n"
                    + "|                  +--Project|      Project|Patterns: us.*, Target DU: unit0000000001|\n"
                    + "|            +--Project      |      Project|Patterns: us.*, Target DU: unit0000000002|\n"
                    + "+----------------------------+-------------+-----------------------------------------+\n"
                    + "Total line number = 13\n"),
            new Pair<>(
                "explain SELECT * FROM us.d1 LEFT OUTER JOIN us.d2 ON us.d1.s1 = us.d2.s1 AND us.d1.s1 < 10 AND us.d2.s1 < 10 WHERE us.d1.s2 > 10;",
                "ResultSets:\n"
                    + "+--------------------------+-------------+----------------------------------------------------------------------------------------------------------------------+\n"
                    + "|              Logical Tree|Operator Type|                                                                                                         Operator Info|\n"
                    + "+--------------------------+-------------+----------------------------------------------------------------------------------------------------------------------+\n"
                    + "|Reorder                   |      Reorder|                                                                                                              Order: *|\n"
                    + "|  +--Project              |      Project|                                                                                                           Patterns: *|\n"
                    + "|    +--OuterJoin          |    OuterJoin|PrefixA: us.d1, PrefixB: us.d2, OuterJoinType: LEFT, IsNatural: false, Filter: (us.d1.s1 == us.d2.s1 && us.d1.s1 < 10)|\n"
                    + "|      +--Select           |       Select|                                                                                               Filter: (us.d1.s2 > 10)|\n"
                    + "|        +--Join           |         Join|                                                                                                           JoinBy: key|\n"
                    + "|          +--Select       |       Select|                                                                                               Filter: (us.d1.s2 > 10)|\n"
                    + "|            +--Join       |         Join|                                                                                                           JoinBy: key|\n"
                    + "|              +--Select   |       Select|                                                                                               Filter: (us.d1.s2 > 10)|\n"
                    + "|                +--Project|      Project|                                                                          Patterns: us.d1.*, Target DU: unit0000000000|\n"
                    + "|              +--Project  |      Project|                                                                          Patterns: us.d1.*, Target DU: unit0000000001|\n"
                    + "|          +--Project      |      Project|                                                                          Patterns: us.d1.*, Target DU: unit0000000002|\n"
                    + "|      +--Select           |       Select|                                                                                               Filter: (us.d2.s1 < 10)|\n"
                    + "|        +--Project        |      Project|                                                                          Patterns: us.d2.*, Target DU: unit0000000001|\n"
                    + "+--------------------------+-------------+----------------------------------------------------------------------------------------------------------------------+\n"
                    + "Total line number = 13\n"),
            new Pair<>(
                "explain SELECT * FROM us.d1, us.d2, us.d3 WHERE us.d1.s1 = us.d2.s1 AND us.d2.s1 = us.d3.s1 AND us.d2.s1 < 10;",
                "ResultSets:\n"
                    + "+----------------------+-------------+--------------------------------------------------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                                                                   Operator Info|\n"
                    + "+----------------------+-------------+--------------------------------------------------------------------------------+\n"
                    + "|Reorder               |      Reorder|                                                                        Order: *|\n"
                    + "|  +--Project          |      Project|                                                                     Patterns: *|\n"
                    + "|    +--InnerJoin      |    InnerJoin|PrefixA: us.d2, PrefixB: us.d3, IsNatural: false, Filter: (us.d2.s1 == us.d3.s1)|\n"
                    + "|      +--InnerJoin    |    InnerJoin|PrefixA: us.d1, PrefixB: us.d2, IsNatural: false, Filter: (us.d1.s1 == us.d2.s1)|\n"
                    + "|        +--Join       |         Join|                                                                     JoinBy: key|\n"
                    + "|          +--Join     |         Join|                                                                     JoinBy: key|\n"
                    + "|            +--Project|      Project|                                    Patterns: us.d1.*, Target DU: unit0000000000|\n"
                    + "|            +--Project|      Project|                                    Patterns: us.d1.*, Target DU: unit0000000001|\n"
                    + "|          +--Project  |      Project|                                    Patterns: us.d1.*, Target DU: unit0000000002|\n"
                    + "|        +--Select     |       Select|                                                         Filter: (us.d2.s1 < 10)|\n"
                    + "|          +--Project  |      Project|                                    Patterns: us.d2.*, Target DU: unit0000000001|\n"
                    + "|      +--Project      |      Project|                                    Patterns: us.d3.*, Target DU: unit0000000001|\n"
                    + "+----------------------+-------------+--------------------------------------------------------------------------------+\n"
                    + "Total line number = 12\n"),
            new Pair<>(
                "explain SELECT * FROM (SELECT max(s2), min(s3), s1 FROM us.d1 GROUP BY s1) WHERE us.d1.s1 < 10 AND max(us.d1.s2) > 10;",
                "ResultSets:\n"
                    + "+----------------------+-------------+-----------------------------------------------------------------------------------------------------------------------+\n"
                    + "|          Logical Tree|Operator Type|                                                                                                          Operator Info|\n"
                    + "+----------------------+-------------+-----------------------------------------------------------------------------------------------------------------------+\n"
                    + "|Reorder               |      Reorder|                                                                                                               Order: *|\n"
                    + "|  +--Project          |      Project|                                                                                                            Patterns: *|\n"
                    + "|    +--Reorder        |      Reorder|                                                                            Order: max(us.d1.s2),min(us.d1.s3),us.d1.s1|\n"
                    + "|      +--Select       |       Select|                                                                                           Filter: (max(us.d1.s2) > 10)|\n"
                    + "|        +--GroupBy    |      GroupBy|GroupByCols: us.d1.s1, FuncList(Name, FuncType): (min, System),(max, System), MappingType: SetMapping isDistinct: false|\n"
                    + "|          +--Select   |       Select|                                                                                                Filter: (us.d1.s1 < 10)|\n"
                    + "|            +--Project|      Project|                                                        Patterns: us.d1.s1,us.d1.s2,us.d1.s3, Target DU: unit0000000000|\n"
                    + "+----------------------+-------------+-----------------------------------------------------------------------------------------------------------------------+\n"
                    + "Total line number = 7\n"),
            new Pair<>(
                "explain SELECT * FROM (SELECT avg(s2), count(s3) FROM us.d1) WHERE avg(us.d1.s2) < 10;",
                "ResultSets:\n"
                    + "+-----------------------+-------------+----------------------------------------------------------------------------------------------------+\n"
                    + "|           Logical Tree|Operator Type|                                                                                       Operator Info|\n"
                    + "+-----------------------+-------------+----------------------------------------------------------------------------------------------------+\n"
                    + "|Reorder                |      Reorder|                                                                                            Order: *|\n"
                    + "|  +--Project           |      Project|                                                                                         Patterns: *|\n"
                    + "|    +--Reorder         |      Reorder|                                                                Order: avg(us.d1.s2),count(us.d1.s3)|\n"
                    + "|      +--Select        |       Select|                                                                          Filter: avg(us.d1.s2) < 10|\n"
                    + "|        +--SetTransform| SetTransform|FuncList(Name, FuncType): (avg, System), (count, System), MappingType: SetMapping, isDistinct: false|\n"
                    + "|          +--Project   |      Project|                                              Patterns: us.d1.s2,us.d1.s3, Target DU: unit0000000000|\n"
                    + "+-----------------------+-------------+----------------------------------------------------------------------------------------------------+\n"
                    + "Total line number = 6\n"),
            new Pair<>(
                "explain SELECT s1 FROM us.d1 WHERE s1 < 10 && EXISTS (SELECT s1 FROM us.d2 WHERE us.d1.s1 > us.d2.s1);",
                "ResultSets:\n"
                    + "+--------------------------+-------------+-------------------------------------------------------------------------------+\n"
                    + "|              Logical Tree|Operator Type|                                                                  Operator Info|\n"
                    + "+--------------------------+-------------+-------------------------------------------------------------------------------+\n"
                    + "|Reorder                   |      Reorder|                                                                Order: us.d1.s1|\n"
                    + "|  +--Project              |      Project|                                                             Patterns: us.d1.s1|\n"
                    + "|    +--Select             |       Select|                                                      Filter: (&mark11 == true)|\n"
                    + "|      +--MarkJoin         |     MarkJoin|Filter: True, MarkColumn: &mark11, IsAntiJoin: false, ExtraJoinPrefix: us.d1.s1|\n"
                    + "|        +--Select         |       Select|                                                        Filter: (us.d1.s1 < 10)|\n"
                    + "|          +--Project      |      Project|                                  Patterns: us.d1.s1, Target DU: unit0000000000|\n"
                    + "|        +--Project        |      Project|                                                    Patterns: us.d1.s1,us.d2.s1|\n"
                    + "|          +--InnerJoin    |    InnerJoin|  PrefixA: null, PrefixB: null, IsNatural: false, Filter: (us.d1.s1 > us.d2.s1)|\n"
                    + "|            +--Project    |      Project|                                                             Patterns: us.d1.s1|\n"
                    + "|              +--Select   |       Select|                                                        Filter: (us.d1.s1 < 10)|\n"
                    + "|                +--Project|      Project|                                  Patterns: us.d1.s1, Target DU: unit0000000000|\n"
                    + "|            +--Project    |      Project|                                  Patterns: us.d2.s1, Target DU: unit0000000001|\n"
                    + "+--------------------------+-------------+-------------------------------------------------------------------------------+\n"
                    + "Total line number = 12\n"),
            new Pair<>(
                "explain SELECT * FROM us.d1 LEFT OUTER JOIN us.d2 ON us.d1.s1 = us.d2.s1 WHERE us.d2.s2 < 10;",
                "ResultSets:\n"
                    + "+--------------------+-------------+------------------------------------------------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                                                                 Operator Info|\n"
                    + "+--------------------+-------------+------------------------------------------------------------------------------+\n"
                    + "|Reorder             |      Reorder|                                                                      Order: *|\n"
                    + "|  +--Project        |      Project|                                                                   Patterns: *|\n"
                    + "|    +--InnerJoin    |    InnerJoin|PrefixA: us.d1, PrefixB: us.d2, IsNatural: false, Filter: us.d1.s1 == us.d2.s1|\n"
                    + "|      +--Join       |         Join|                                                                   JoinBy: key|\n"
                    + "|        +--Join     |         Join|                                                                   JoinBy: key|\n"
                    + "|          +--Project|      Project|                                  Patterns: us.d1.*, Target DU: unit0000000000|\n"
                    + "|          +--Project|      Project|                                  Patterns: us.d1.*, Target DU: unit0000000001|\n"
                    + "|        +--Project  |      Project|                                  Patterns: us.d1.*, Target DU: unit0000000002|\n"
                    + "|      +--Select     |       Select|                                                       Filter: (us.d2.s2 < 10)|\n"
                    + "|        +--Project  |      Project|                                  Patterns: us.d2.*, Target DU: unit0000000001|\n"
                    + "+--------------------+-------------+------------------------------------------------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "explain SELECT * FROM us.d1 FULL OUTER JOIN us.d2 ON us.d1.s1 = us.d2.s1 WHERE us.d1.s2 > 10;",
                "ResultSets:\n"
                    + "+--------------------------+-------------+---------------------------------------------------------------------------------------------------+\n"
                    + "|              Logical Tree|Operator Type|                                                                                      Operator Info|\n"
                    + "+--------------------------+-------------+---------------------------------------------------------------------------------------------------+\n"
                    + "|Reorder                   |      Reorder|                                                                                           Order: *|\n"
                    + "|  +--Project              |      Project|                                                                                        Patterns: *|\n"
                    + "|    +--OuterJoin          |    OuterJoin|PrefixA: us.d1, PrefixB: us.d2, OuterJoinType: LEFT, IsNatural: false, Filter: us.d1.s1 == us.d2.s1|\n"
                    + "|      +--Select           |       Select|                                                                            Filter: (us.d1.s2 > 10)|\n"
                    + "|        +--Join           |         Join|                                                                                        JoinBy: key|\n"
                    + "|          +--Select       |       Select|                                                                            Filter: (us.d1.s2 > 10)|\n"
                    + "|            +--Join       |         Join|                                                                                        JoinBy: key|\n"
                    + "|              +--Select   |       Select|                                                                            Filter: (us.d1.s2 > 10)|\n"
                    + "|                +--Project|      Project|                                                       Patterns: us.d1.*, Target DU: unit0000000000|\n"
                    + "|              +--Project  |      Project|                                                       Patterns: us.d1.*, Target DU: unit0000000001|\n"
                    + "|          +--Project      |      Project|                                                       Patterns: us.d1.*, Target DU: unit0000000002|\n"
                    + "|      +--Project          |      Project|                                                       Patterns: us.d2.*, Target DU: unit0000000001|\n"
                    + "+--------------------------+-------------+---------------------------------------------------------------------------------------------------+\n"
                    + "Total line number = 12\n"));

    for (Pair<String, String> pair : statementsAndExpectRes) {
      String statement = pair.k;
      String expectRes = pair.v;
      String res = executor.execute(statement);

      // 把 &mark数字 后面的数去掉，然后把空格和分隔符去掉，不然因为mark后面的数字是变动的，容易格式匹配不上
      if (expectRes.contains("&mark")) {
        res =
            Arrays.stream(res.split("\n"))
                .filter(s -> !s.startsWith("+"))
                .collect(Collectors.joining("\n"));
        expectRes =
            Arrays.stream(expectRes.split("\n"))
                .filter(s -> !s.startsWith("+"))
                .collect(Collectors.joining("\n"));
        res = res.replaceAll("&mark\\d+", "&mark").replaceAll(" ", "");
        expectRes = expectRes.replaceAll("&mark\\d+", "&mark").replaceAll(" ", "");
      }

      assertEquals(res, expectRes);
    }

    String openRule = "SET RULES FragmentPruningByPatternRule=ON, ColumnPruningRule=ON;";
    executor.execute(openRule);
  }

  @Test
  public void testFilterFragmentOptimizer() {
    String policy = executor.execute("SHOW CONFIG \"policyClassName\";");
    if (!policy.contains("KeyRangeTestPolicy")) {
      LOGGER.info(
          "Skip SQLSessionIT.testFilterFragmentOptimizer because policy is not KeyRangeTestPolicy");
      return;
    }

    if (isFilterPushDown) {
      LOGGER.info(
          "Skip SQLSessionIT.testFilterFragmentOptimizer because optimizer is not remove_not,filter_fragment");
      return;
    }

    if (isScaling) {
      LOGGER.info("Skip SQLSessionIT.testFilterFragmentOptimizer because it is scaling test");
      return;
    }

    String closeRule = "SET RULES FragmentPruningByPatternRule=OFF, ColumnPruningRule=OFF;";
    executor.execute(closeRule);

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

    // 开启filter_fragment
    String statement = "SET RULES FragmentPruningByFilterRule=ON;";
    executor.execute(statement);

    // 这里的测例是包含了filter_fragment不能处理的节点，因此开不开filter_fragment都是一样的结果
    List<Pair<String, String>> statementsAndExpectResNoChange =
        Arrays.asList(
            new Pair<>(
                "EXPLAIN SELECT s1 FROM us.d1 JOIN us.d2 WHERE key < 100;",
                "ResultSets:\n"
                    + "+------------------------+-------------+------------------------------------------------+\n"
                    + "|            Logical Tree|Operator Type|                                   Operator Info|\n"
                    + "+------------------------+-------------+------------------------------------------------+\n"
                    + "|Reorder                 |      Reorder|                                       Order: s1|\n"
                    + "|  +--Project            |      Project|                                    Patterns: s1|\n"
                    + "|    +--Select           |       Select|                               Filter: key < 100|\n"
                    + "|      +--InnerJoin      |    InnerJoin|PrefixA: us.d1, PrefixB: us.d2, IsNatural: false|\n"
                    + "|        +--PathUnion    |    PathUnion|                                                |\n"
                    + "|          +--Join       |         Join|                                     JoinBy: key|\n"
                    + "|            +--Join     |         Join|                                     JoinBy: key|\n"
                    + "|              +--Project|      Project|    Patterns: us.d1.*, Target DU: unit0000000000|\n"
                    + "|              +--Project|      Project|    Patterns: us.d1.*, Target DU: unit0000000002|\n"
                    + "|            +--Project  |      Project|    Patterns: us.d1.*, Target DU: unit0000000004|\n"
                    + "|          +--Join       |         Join|                                     JoinBy: key|\n"
                    + "|            +--Join     |         Join|                                     JoinBy: key|\n"
                    + "|              +--Project|      Project|    Patterns: us.d1.*, Target DU: unit0000000001|\n"
                    + "|              +--Project|      Project|    Patterns: us.d1.*, Target DU: unit0000000003|\n"
                    + "|            +--Project  |      Project|    Patterns: us.d1.*, Target DU: unit0000000005|\n"
                    + "|        +--PathUnion    |    PathUnion|                                                |\n"
                    + "|          +--Project    |      Project|    Patterns: us.d2.*, Target DU: unit0000000002|\n"
                    + "|          +--Project    |      Project|    Patterns: us.d2.*, Target DU: unit0000000003|\n"
                    + "+------------------------+-------------+------------------------------------------------+\n"
                    + "Total line number = 18\n"),
            new Pair<>(
                "EXPLAIN SELECT avg(bb) FROM (SELECT a as aa, b as bb FROM us.d2) WHERE key > 2 GROUP BY aa;",
                "ResultSets:\n"
                    + "+------------------------+-------------+---------------------------------------------------------------------------------------------------+\n"
                    + "|            Logical Tree|Operator Type|                                                                                      Operator Info|\n"
                    + "+------------------------+-------------+---------------------------------------------------------------------------------------------------+\n"
                    + "|Reorder                 |      Reorder|                                                                                     Order: avg(bb)|\n"
                    + "|  +--GroupBy            |      GroupBy|GroupByCols: aa, FuncList(Name, FuncType): (avg, System), MappingType: SetMapping isDistinct: false|\n"
                    + "|    +--Select           |       Select|                                                                                    Filter: key > 2|\n"
                    + "|      +--Rename         |       Rename|                                                              AliasMap: (us.d2.a, aa),(us.d2.b, bb)|\n"
                    + "|        +--Reorder      |      Reorder|                                                                             Order: us.d2.a,us.d2.b|\n"
                    + "|          +--Project    |      Project|                                                                          Patterns: us.d2.a,us.d2.b|\n"
                    + "|            +--PathUnion|    PathUnion|                                                                                                   |\n"
                    + "|              +--Project|      Project|                                               Patterns: us.d2.a,us.d2.b, Target DU: unit0000000002|\n"
                    + "|              +--Project|      Project|                                               Patterns: us.d2.a,us.d2.b, Target DU: unit0000000003|\n"
                    + "+------------------------+-------------+---------------------------------------------------------------------------------------------------+\n"
                    + "Total line number = 9\n"));

    // 这里的测例是filter_fragment能处理的节点，开关会导致变化
    List<Pair<String, String>> statementsAndExpectResAfterOptimize =
        Arrays.asList(
            new Pair<>(
                "explain SELECT COUNT(*)\n"
                    + "FROM (\n"
                    + "    SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2\n"
                    + "    FROM us.d1 OVER WINDOW (size 10 IN [1000, 1100))\n"
                    + ")\n"
                    + "OVER WINDOW (size 20 IN [1000, 1100));",
                "ResultSets:\n"
                    + "+------------------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    + "|            Logical Tree|Operator Type|                                                                                                                                 Operator Info|\n"
                    + "+------------------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    + "|Reorder                 |      Reorder|                                                                                                                               Order: count(*)|\n"
                    + "|  +--Downsample         |   Downsample|             Precision: 20, SlideDistance: 20, TimeRange: [1000, 1100), FuncList(Name, FunctionType): (count, System), MappingType: SetMapping|\n"
                    + "|    +--Select           |       Select|                                                                                                           Filter: (key >= 1000 && key < 1100)|\n"
                    + "|      +--Rename         |       Rename|                                                                                     AliasMap: (avg(us.d1.s1), avg_s1),(sum(us.d1.s2), sum_s2)|\n"
                    + "|        +--Reorder      |      Reorder|                                                                                                            Order: avg(us.d1.s1),sum(us.d1.s2)|\n"
                    + "|          +--Downsample |   Downsample|Precision: 10, SlideDistance: 10, TimeRange: [1000, 1100), FuncList(Name, FunctionType): (avg, System), (sum, System), MappingType: SetMapping|\n"
                    + "|            +--Select   |       Select|                                                                                                           Filter: (key >= 1000 && key < 1100)|\n"
                    + "|              +--Project|      Project|                                                                                        Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000000|\n"
                    + "+------------------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    + "Total line number = 8\n"),
            new Pair<>(
                "EXPLAIN SELECT d1.* FROM us where key < 10;",
                "ResultSets:\n"
                    + "+--------------------+-------------+--------------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                               Operator Info|\n"
                    + "+--------------------+-------------+--------------------------------------------+\n"
                    + "|Reorder             |      Reorder|                              Order: us.d1.*|\n"
                    + "|  +--Project        |      Project|                           Patterns: us.d1.*|\n"
                    + "|    +--Select       |       Select|                            Filter: key < 10|\n"
                    + "|      +--Join       |         Join|                                 JoinBy: key|\n"
                    + "|        +--Join     |         Join|                                 JoinBy: key|\n"
                    + "|          +--Project|      Project|Patterns: us.d1.*, Target DU: unit0000000000|\n"
                    + "|          +--Project|      Project|Patterns: us.d1.*, Target DU: unit0000000002|\n"
                    + "|        +--Project  |      Project|Patterns: us.d1.*, Target DU: unit0000000004|\n"
                    + "+--------------------+-------------+--------------------------------------------+\n"
                    + "Total line number = 8\n"),
            new Pair<>(
                "EXPLAIN SELECT d2.c FROM us where key < 10;",
                "ResultSets:\n"
                    + "+----------------+-------------+--------------------------------------------+\n"
                    + "|    Logical Tree|Operator Type|                               Operator Info|\n"
                    + "+----------------+-------------+--------------------------------------------+\n"
                    + "|Reorder         |      Reorder|                              Order: us.d2.c|\n"
                    + "|  +--Project    |      Project|                           Patterns: us.d2.c|\n"
                    + "|    +--Select   |       Select|                            Filter: key < 10|\n"
                    + "|      +--Project|      Project|Patterns: us.d2.c, Target DU: unit0000000002|\n"
                    + "+----------------+-------------+--------------------------------------------+\n"
                    + "Total line number = 4\n"));

    executor.concurrentExecuteAndCompare(statementsAndExpectResAfterOptimize);
    executor.concurrentExecuteAndCompare(statementsAndExpectResNoChange);

    // 关闭filter_fragment
    statement = "SET RULES FragmentPruningByFilterRule=OFF;";
    executor.execute(statement);

    List<Pair<String, String>> statementsAndExpectResBeforeOptimize =
        Arrays.asList(
            new Pair<>(
                "explain SELECT COUNT(*)\n"
                    + "FROM (\n"
                    + "    SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2\n"
                    + "    FROM us.d1 OVER WINDOW (size 10 IN [1000, 1100))\n"
                    + ")\n"
                    + "OVER WINDOW (size 20 IN [1000, 1100));",
                "ResultSets:\n"
                    + "+--------------------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    + "|              Logical Tree|Operator Type|                                                                                                                                 Operator Info|\n"
                    + "+--------------------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    + "|Reorder                   |      Reorder|                                                                                                                               Order: count(*)|\n"
                    + "|  +--Downsample           |   Downsample|             Precision: 20, SlideDistance: 20, TimeRange: [1000, 1100), FuncList(Name, FunctionType): (count, System), MappingType: SetMapping|\n"
                    + "|    +--Select             |       Select|                                                                                                           Filter: (key >= 1000 && key < 1100)|\n"
                    + "|      +--Rename           |       Rename|                                                                                     AliasMap: (avg(us.d1.s1), avg_s1),(sum(us.d1.s2), sum_s2)|\n"
                    + "|        +--Reorder        |      Reorder|                                                                                                            Order: avg(us.d1.s1),sum(us.d1.s2)|\n"
                    + "|          +--Downsample   |   Downsample|Precision: 10, SlideDistance: 10, TimeRange: [1000, 1100), FuncList(Name, FunctionType): (avg, System), (sum, System), MappingType: SetMapping|\n"
                    + "|            +--Select     |       Select|                                                                                                           Filter: (key >= 1000 && key < 1100)|\n"
                    + "|              +--PathUnion|    PathUnion|                                                                                                                                              |\n"
                    + "|                +--Project|      Project|                                                                                        Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000000|\n"
                    + "|                +--Project|      Project|                                                                                        Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000001|\n"
                    + "+--------------------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    + "Total line number = 10\n"),
            new Pair<>(
                "EXPLAIN SELECT d1.* FROM us;",
                "ResultSets:\n"
                    + "+--------------------+-------------+--------------------------------------------+\n"
                    + "|        Logical Tree|Operator Type|                               Operator Info|\n"
                    + "+--------------------+-------------+--------------------------------------------+\n"
                    + "|Reorder             |      Reorder|                              Order: us.d1.*|\n"
                    + "|  +--Project        |      Project|                           Patterns: us.d1.*|\n"
                    + "|    +--PathUnion    |    PathUnion|                                            |\n"
                    + "|      +--Join       |         Join|                                 JoinBy: key|\n"
                    + "|        +--Join     |         Join|                                 JoinBy: key|\n"
                    + "|          +--Project|      Project|Patterns: us.d1.*, Target DU: unit0000000000|\n"
                    + "|          +--Project|      Project|Patterns: us.d1.*, Target DU: unit0000000002|\n"
                    + "|        +--Project  |      Project|Patterns: us.d1.*, Target DU: unit0000000004|\n"
                    + "|      +--Join       |         Join|                                 JoinBy: key|\n"
                    + "|        +--Join     |         Join|                                 JoinBy: key|\n"
                    + "|          +--Project|      Project|Patterns: us.d1.*, Target DU: unit0000000001|\n"
                    + "|          +--Project|      Project|Patterns: us.d1.*, Target DU: unit0000000003|\n"
                    + "|        +--Project  |      Project|Patterns: us.d1.*, Target DU: unit0000000005|\n"
                    + "+--------------------+-------------+--------------------------------------------+\n"
                    + "Total line number = 13\n"),
            new Pair<>(
                "EXPLAIN SELECT d2.c FROM us;",
                "ResultSets:\n"
                    + "+----------------+-------------+--------------------------------------------+\n"
                    + "|    Logical Tree|Operator Type|                               Operator Info|\n"
                    + "+----------------+-------------+--------------------------------------------+\n"
                    + "|Reorder         |      Reorder|                              Order: us.d2.c|\n"
                    + "|  +--Project    |      Project|                           Patterns: us.d2.c|\n"
                    + "|    +--PathUnion|    PathUnion|                                            |\n"
                    + "|      +--Project|      Project|Patterns: us.d2.c, Target DU: unit0000000002|\n"
                    + "|      +--Project|      Project|Patterns: us.d2.c, Target DU: unit0000000003|\n"
                    + "+----------------+-------------+--------------------------------------------+\n"
                    + "Total line number = 5\n"));

    executor.concurrentExecuteAndCompare(statementsAndExpectResBeforeOptimize);
    executor.concurrentExecuteAndCompare(statementsAndExpectResNoChange);

    // 开启filter_fragment
    statement =
        "SET RULES FragmentPruningByFilterRule=ON, FragmentPruningByPatternRule=ON, ColumnPruningRule=ON;";
    executor.execute(statement);
  }

  @Test
  public void testFilterWithMultiTable() {
    String insert1 = "INSERT INTO test.a(key, a) VALUES (1, 1), (2, 2), (3, 3);";
    String insert2 = "INSERT INTO test.b(key, a) VALUES (1, 1), (2, 2), (3, 3);";
    String insert3 = "INSERT INTO test.c(key, a) VALUES (1, 1), (2, 2), (3, 3);";

    executor.execute(insert1);
    executor.execute(insert2);
    executor.execute(insert3);

    String query = "SELECT * FROM test WHERE a.a < 3;";
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

  @Test
  public void testSetMappingTransform() {
    String query = "SELECT max(s1), min(s2) from us.d1;";
    String expect =
        "ResultSets:\n"
            + "+-------------+-------------+\n"
            + "|max(us.d1.s1)|min(us.d1.s2)|\n"
            + "+-------------+-------------+\n"
            + "|        14999|            1|\n"
            + "+-------------+-------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expect);

    query = "SELECT count(s1), avg(s2) from us.d1;";
    expect =
        "ResultSets:\n"
            + "+---------------+-------------+\n"
            + "|count(us.d1.s1)|avg(us.d1.s2)|\n"
            + "+---------------+-------------+\n"
            + "|          15000|       7500.5|\n"
            + "+---------------+-------------+\n"
            + "Total line number = 1\n";
    executor.executeAndCompare(query, expect);

    if (!isScaling) {
      query = "explain SELECT count(s1), avg(s2) from us.d1;";
      expect =
          "ResultSets:\n"
              + "+-----------------+-------------+----------------------------------------------------------------------------------------------------+\n"
              + "|     Logical Tree|Operator Type|                                                                                       Operator Info|\n"
              + "+-----------------+-------------+----------------------------------------------------------------------------------------------------+\n"
              + "|Reorder          |      Reorder|                                                                Order: count(us.d1.s1),avg(us.d1.s2)|\n"
              + "|  +--SetTransform| SetTransform|FuncList(Name, FuncType): (avg, System), (count, System), MappingType: SetMapping, isDistinct: false|\n"
              + "|    +--Project   |      Project|                                              Patterns: us.d1.s1,us.d1.s2, Target DU: unit0000000000|\n"
              + "+-----------------+-------------+----------------------------------------------------------------------------------------------------+\n"
              + "Total line number = 3\n";
      executor.executeAndCompare(query, expect);
    }
  }

  @Test
  public void testMappingTransform() {
    String query = "SELECT first(s1), last(s2) from us.d1;";
    String expect =
        "ResultSets:\n"
            + "+-----+--------+-----+\n"
            + "|  key|    path|value|\n"
            + "+-----+--------+-----+\n"
            + "|    0|us.d1.s1|    0|\n"
            + "|14999|us.d1.s2|15000|\n"
            + "+-----+--------+-----+\n"
            + "Total line number = 2\n";
    executor.executeAndCompare(query, expect);

    query = "SELECT first(s1), last(s2), last(s4) from us.d1;";
    expect =
        "ResultSets:\n"
            + "+-----+--------+-------+\n"
            + "|  key|    path|  value|\n"
            + "+-----+--------+-------+\n"
            + "|    0|us.d1.s1|      0|\n"
            + "|14999|us.d1.s2|  15000|\n"
            + "|14999|us.d1.s4|14999.1|\n"
            + "+-----+--------+-------+\n"
            + "Total line number = 3\n";
    executor.executeAndCompare(query, expect);

    if (!isScaling) {
      query = "explain SELECT first(s1), last(s2), first(s3), last(s4) from us.d1;";
      expect =
          "ResultSets:\n"
              + "+---------------------+----------------+----------------------------------------------------------------------------------------------------------------+\n"
              + "|         Logical Tree|   Operator Type|                                                                                                   Operator Info|\n"
              + "+---------------------+----------------+----------------------------------------------------------------------------------------------------------------+\n"
              + "|Reorder              |         Reorder|                                                                                               Order: path,value|\n"
              + "|  +--MappingTransform|MappingTransform|FuncList(Name, FuncType): (last, System), (last, System), (first, System), (first, System), MappingType: Mapping|\n"
              + "|    +--Join          |            Join|                                                                                                     JoinBy: key|\n"
              + "|      +--Project     |         Project|                                                 Patterns: us.d1.s1,us.d1.s2,us.d1.s3, Target DU: unit0000000000|\n"
              + "|      +--Project     |         Project|                                                                   Patterns: us.d1.s4, Target DU: unit0000000001|\n"
              + "+---------------------+----------------+----------------------------------------------------------------------------------------------------------------+\n"
              + "Total line number = 5\n";
      executor.executeAndCompare(query, expect);
    }
  }

  @Test
  public void testColumnPruningAndFragmentPruning() {
    if (isFilterPushDown || isScaling) {
      LOGGER.info(
          "Skip SQLSessionIT.testColumnPruningAndFragmentPruning because scaling test or filter push down test");
      return;
    }

    StringBuilder insert =
        new StringBuilder(
            "INSERT INTO test(key, a.a, a.b, a.c, a.d, a.e, b.f, b.g, b.h, b.i, b.j, b.k) VALUES ");
    int rows = 100;
    for (int i = 0; i < rows; i++) {
      insert
          .append("(")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(",")
          .append(i)
          .append(")")
          .append(",");
    }

    insert.deleteCharAt(insert.length() - 1);
    insert.append(";");
    executor.execute(insert.toString());

    String closeRule = "SET RULES ColumnPruningRule=OFF, FragmentPruningByPatternRule=OFF;";
    executor.execute(closeRule);

    String sql1 = "explain SELECT us.d1.s1 FROM (SELECT * FROM us.d1);";
    String sql2 = "explain SELECT test.a.a FROM test.a INNER JOIN us.d1 ON test.a.b = us.d1.s1;";
    String sql3 = "explain SELECT test.a.a, test.a.e, test.b.k FROM (SELECT * FROM test);";
    String sql4 =
        "explain SELECT test.a.a, (SELECT AVG(b) FROM test.a) FROM (SELECT * FROM test.a);";
    String sql5 =
        "explain select test.a.a, test.a.b from (select * from test.a UNION select f,g,h,i,j  from test.b);";
    String sql6 =
        "explain select test.a.a, test.a.b from \n"
            + "(select a,b,c,d from test.a WHERE test.a.a < 50000 INTERSECT select f,g,h,i from test.b WHERE test.b.f > 30000);";

    String expect1 =
        "ResultSets:\n"
            + "+----------------------+-------------+--------------------------------------------+\n"
            + "|          Logical Tree|Operator Type|                               Operator Info|\n"
            + "+----------------------+-------------+--------------------------------------------+\n"
            + "|Reorder               |      Reorder|                             Order: us.d1.s1|\n"
            + "|  +--Project          |      Project|                          Patterns: us.d1.s1|\n"
            + "|    +--Reorder        |      Reorder|                              Order: us.d1.*|\n"
            + "|      +--Project      |      Project|                           Patterns: us.d1.*|\n"
            + "|        +--Join       |         Join|                                 JoinBy: key|\n"
            + "|          +--Join     |         Join|                                 JoinBy: key|\n"
            + "|            +--Project|      Project|Patterns: us.d1.*, Target DU: unit0000000000|\n"
            + "|            +--Project|      Project|Patterns: us.d1.*, Target DU: unit0000000001|\n"
            + "|          +--Project  |      Project|Patterns: us.d1.*, Target DU: unit0000000002|\n"
            + "+----------------------+-------------+--------------------------------------------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(sql1, expect1);

    String expect2 =
        "ResultSets:\n"
            + "+--------------------+-------------+-------------------------------------------------------------------------------+\n"
            + "|        Logical Tree|Operator Type|                                                                  Operator Info|\n"
            + "+--------------------+-------------+-------------------------------------------------------------------------------+\n"
            + "|Reorder             |      Reorder|                                                                Order: test.a.a|\n"
            + "|  +--Project        |      Project|                                                             Patterns: test.a.a|\n"
            + "|    +--InnerJoin    |    InnerJoin|PrefixA: test.a, PrefixB: us.d1, IsNatural: false, Filter: test.a.b == us.d1.s1|\n"
            + "|      +--Project    |      Project|                                  Patterns: test.a.*, Target DU: unit0000000002|\n"
            + "|      +--Join       |         Join|                                                                    JoinBy: key|\n"
            + "|        +--Join     |         Join|                                                                    JoinBy: key|\n"
            + "|          +--Project|      Project|                                   Patterns: us.d1.*, Target DU: unit0000000000|\n"
            + "|          +--Project|      Project|                                   Patterns: us.d1.*, Target DU: unit0000000001|\n"
            + "|        +--Project  |      Project|                                   Patterns: us.d1.*, Target DU: unit0000000002|\n"
            + "+--------------------+-------------+-------------------------------------------------------------------------------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(sql2, expect2);

    String expect3 =
        "ResultSets:\n"
            + "+------------------+-------------+-------------------------------------------+\n"
            + "|      Logical Tree|Operator Type|                              Operator Info|\n"
            + "+------------------+-------------+-------------------------------------------+\n"
            + "|Reorder           |      Reorder|          Order: test.a.a,test.a.e,test.b.k|\n"
            + "|  +--Project      |      Project|       Patterns: test.b.k,test.a.e,test.a.a|\n"
            + "|    +--Reorder    |      Reorder|                              Order: test.*|\n"
            + "|      +--Project  |      Project|                           Patterns: test.*|\n"
            + "|        +--Project|      Project|Patterns: test.*, Target DU: unit0000000002|\n"
            + "+------------------+-------------+-------------------------------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(sql3, expect3);

    String expect4 =
        "ResultSets:\n"
            + "+-----------------------+-------------+-----------------------------------------------------------------------------------+\n"
            + "|           Logical Tree|Operator Type|                                                                      Operator Info|\n"
            + "+-----------------------+-------------+-----------------------------------------------------------------------------------+\n"
            + "|Reorder                |      Reorder|                                                      Order: test.a.a,avg(test.a.b)|\n"
            + "|  +--Project           |      Project|                                                   Patterns: avg(test.a.b),test.a.a|\n"
            + "|    +--SingleJoin      |   SingleJoin|                                                                       Filter: True|\n"
            + "|      +--Reorder       |      Reorder|                                                                    Order: test.a.*|\n"
            + "|        +--Project     |      Project|                                                                 Patterns: test.a.*|\n"
            + "|          +--Project   |      Project|                                      Patterns: test.a.*, Target DU: unit0000000002|\n"
            + "|      +--Reorder       |      Reorder|                                                               Order: avg(test.a.b)|\n"
            + "|        +--SetTransform| SetTransform|FuncList(Name, FuncType): (avg, System), MappingType: SetMapping, isDistinct: false|\n"
            + "|          +--Project   |      Project|                                      Patterns: test.a.b, Target DU: unit0000000002|\n"
            + "+-----------------------+-------------+-----------------------------------------------------------------------------------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(sql4, expect4);

    String expect5 =
        "ResultSets:\n"
            + "+--------------------+-------------+-----------------------------------------------------------------------------------------------+\n"
            + "|        Logical Tree|Operator Type|                                                                                  Operator Info|\n"
            + "+--------------------+-------------+-----------------------------------------------------------------------------------------------+\n"
            + "|Reorder             |      Reorder|                                                                       Order: test.a.a,test.a.b|\n"
            + "|  +--Project        |      Project|                                                                    Patterns: test.a.a,test.a.b|\n"
            + "|    +--Union        |        Union|LeftOrder: test.a.*, RightOrder: test.b.f,test.b.g,test.b.h,test.b.i,test.b.j, isDistinct: true|\n"
            + "|      +--Reorder    |      Reorder|                                                                                Order: test.a.*|\n"
            + "|        +--Project  |      Project|                                                                             Patterns: test.a.*|\n"
            + "|          +--Project|      Project|                                                  Patterns: test.a.*, Target DU: unit0000000002|\n"
            + "|      +--Reorder    |      Reorder|                                            Order: test.b.f,test.b.g,test.b.h,test.b.i,test.b.j|\n"
            + "|        +--Project  |      Project|                                         Patterns: test.b.h,test.b.i,test.b.j,test.b.f,test.b.g|\n"
            + "|          +--Project|      Project|              Patterns: test.b.f,test.b.g,test.b.h,test.b.i,test.b.j, Target DU: unit0000000002|\n"
            + "+--------------------+-------------+-----------------------------------------------------------------------------------------------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(sql5, expect5);

    String expect6 =
        "ResultSets:\n"
            + "+----------------------+-------------+-----------------------------------------------------------------------------------------------------------------+\n"
            + "|          Logical Tree|Operator Type|                                                                                                    Operator Info|\n"
            + "+----------------------+-------------+-----------------------------------------------------------------------------------------------------------------+\n"
            + "|Reorder               |      Reorder|                                                                                         Order: test.a.a,test.a.b|\n"
            + "|  +--Project          |      Project|                                                                                      Patterns: test.a.a,test.a.b|\n"
            + "|    +--Intersect      |    Intersect|LeftOrder: test.a.a,test.a.b,test.a.c,test.a.d, RightOrder: test.b.f,test.b.g,test.b.h,test.b.i, isDistinct: true|\n"
            + "|      +--Reorder      |      Reorder|                                                                       Order: test.a.a,test.a.b,test.a.c,test.a.d|\n"
            + "|        +--Project    |      Project|                                                                    Patterns: test.a.a,test.a.b,test.a.c,test.a.d|\n"
            + "|          +--Select   |       Select|                                                                                         Filter: test.a.a < 50000|\n"
            + "|            +--Project|      Project|                                         Patterns: test.a.a,test.a.b,test.a.c,test.a.d, Target DU: unit0000000002|\n"
            + "|      +--Reorder      |      Reorder|                                                                       Order: test.b.f,test.b.g,test.b.h,test.b.i|\n"
            + "|        +--Project    |      Project|                                                                    Patterns: test.b.h,test.b.i,test.b.f,test.b.g|\n"
            + "|          +--Select   |       Select|                                                                                         Filter: test.b.f > 30000|\n"
            + "|            +--Project|      Project|                                         Patterns: test.b.f,test.b.g,test.b.h,test.b.i, Target DU: unit0000000002|\n"
            + "+----------------------+-------------+-----------------------------------------------------------------------------------------------------------------+\n"
            + "Total line number = 11\n";
    executor.executeAndCompare(sql6, expect6);

    String openRule = "SET RULES ColumnPruningRule=ON, FragmentPruningByPatternRule=ON;";
    executor.execute(openRule);

    expect1 =
        "ResultSets:\n"
            + "+------------------+-------------+---------------------------------------------+\n"
            + "|      Logical Tree|Operator Type|                                Operator Info|\n"
            + "+------------------+-------------+---------------------------------------------+\n"
            + "|Reorder           |      Reorder|                              Order: us.d1.s1|\n"
            + "|  +--Project      |      Project|                           Patterns: us.d1.s1|\n"
            + "|    +--Reorder    |      Reorder|                              Order: us.d1.s1|\n"
            + "|      +--Project  |      Project|                           Patterns: us.d1.s1|\n"
            + "|        +--Project|      Project|Patterns: us.d1.s1, Target DU: unit0000000000|\n"
            + "+------------------+-------------+---------------------------------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(sql1, expect1);

    expect2 =
        "ResultSets:\n"
            + "+----------------+-------------+-------------------------------------------------------------------------------+\n"
            + "|    Logical Tree|Operator Type|                                                                  Operator Info|\n"
            + "+----------------+-------------+-------------------------------------------------------------------------------+\n"
            + "|Reorder         |      Reorder|                                                                Order: test.a.a|\n"
            + "|  +--Project    |      Project|                                                             Patterns: test.a.a|\n"
            + "|    +--InnerJoin|    InnerJoin|PrefixA: test.a, PrefixB: us.d1, IsNatural: false, Filter: test.a.b == us.d1.s1|\n"
            + "|      +--Project|      Project|                         Patterns: test.a.a,test.a.b, Target DU: unit0000000002|\n"
            + "|      +--Project|      Project|                                  Patterns: us.d1.s1, Target DU: unit0000000000|\n"
            + "+----------------+-------------+-------------------------------------------------------------------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(sql2, expect2);

    expect3 =
        "ResultSets:\n"
            + "+------------------+-------------+---------------------------------------------------------------+\n"
            + "|      Logical Tree|Operator Type|                                                  Operator Info|\n"
            + "+------------------+-------------+---------------------------------------------------------------+\n"
            + "|Reorder           |      Reorder|                              Order: test.a.a,test.a.e,test.b.k|\n"
            + "|  +--Project      |      Project|                           Patterns: test.a.a,test.a.e,test.b.k|\n"
            + "|    +--Reorder    |      Reorder|                              Order: test.a.a,test.a.e,test.b.k|\n"
            + "|      +--Project  |      Project|                           Patterns: test.a.a,test.a.e,test.b.k|\n"
            + "|        +--Project|      Project|Patterns: test.a.a,test.a.e,test.b.k, Target DU: unit0000000002|\n"
            + "+------------------+-------------+---------------------------------------------------------------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(sql3, expect3);

    expect4 =
        "ResultSets:\n"
            + "+-----------------------+-------------+-----------------------------------------------------------------------------------+\n"
            + "|           Logical Tree|Operator Type|                                                                      Operator Info|\n"
            + "+-----------------------+-------------+-----------------------------------------------------------------------------------+\n"
            + "|Reorder                |      Reorder|                                                      Order: test.a.a,avg(test.a.b)|\n"
            + "|  +--Project           |      Project|                                                   Patterns: avg(test.a.b),test.a.a|\n"
            + "|    +--SingleJoin      |   SingleJoin|                                                                       Filter: True|\n"
            + "|      +--Reorder       |      Reorder|                                                                    Order: test.a.a|\n"
            + "|        +--Project     |      Project|                                                                 Patterns: test.a.a|\n"
            + "|          +--Project   |      Project|                                      Patterns: test.a.a, Target DU: unit0000000002|\n"
            + "|      +--Reorder       |      Reorder|                                                               Order: avg(test.a.b)|\n"
            + "|        +--SetTransform| SetTransform|FuncList(Name, FuncType): (avg, System), MappingType: SetMapping, isDistinct: false|\n"
            + "|          +--Project   |      Project|                                      Patterns: test.a.b, Target DU: unit0000000002|\n"
            + "+-----------------------+-------------+-----------------------------------------------------------------------------------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(sql4, expect4);

    expect5 =
        "ResultSets:\n"
            + "+--------------------+-------------+-----------------------------------------------------------------------------+\n"
            + "|        Logical Tree|Operator Type|                                                                Operator Info|\n"
            + "+--------------------+-------------+-----------------------------------------------------------------------------+\n"
            + "|Reorder             |      Reorder|                                                     Order: test.a.a,test.a.b|\n"
            + "|  +--Project        |      Project|                                                  Patterns: test.a.a,test.a.b|\n"
            + "|    +--Union        |        Union|LeftOrder: test.a.a,test.a.b, RightOrder: test.b.f,test.b.g, isDistinct: true|\n"
            + "|      +--Reorder    |      Reorder|                                                     Order: test.a.a,test.a.b|\n"
            + "|        +--Project  |      Project|                                                  Patterns: test.a.a,test.a.b|\n"
            + "|          +--Project|      Project|                       Patterns: test.a.a,test.a.b, Target DU: unit0000000002|\n"
            + "|      +--Reorder    |      Reorder|                                                     Order: test.b.f,test.b.g|\n"
            + "|        +--Project  |      Project|                                                  Patterns: test.b.f,test.b.g|\n"
            + "|          +--Project|      Project|                       Patterns: test.b.f,test.b.g, Target DU: unit0000000002|\n"
            + "+--------------------+-------------+-----------------------------------------------------------------------------+\n"
            + "Total line number = 9\n";
    executor.executeAndCompare(sql5, expect5);

    expect6 =
        "ResultSets:\n"
            + "+----------------------+-------------+-----------------------------------------------------------------------------+\n"
            + "|          Logical Tree|Operator Type|                                                                Operator Info|\n"
            + "+----------------------+-------------+-----------------------------------------------------------------------------+\n"
            + "|Reorder               |      Reorder|                                                     Order: test.a.a,test.a.b|\n"
            + "|  +--Project          |      Project|                                                  Patterns: test.a.a,test.a.b|\n"
            + "|    +--Intersect      |    Intersect|LeftOrder: test.a.a,test.a.b, RightOrder: test.b.f,test.b.g, isDistinct: true|\n"
            + "|      +--Reorder      |      Reorder|                                                     Order: test.a.a,test.a.b|\n"
            + "|        +--Project    |      Project|                                                  Patterns: test.a.a,test.a.b|\n"
            + "|          +--Select   |       Select|                                                     Filter: test.a.a < 50000|\n"
            + "|            +--Project|      Project|                       Patterns: test.a.a,test.a.b, Target DU: unit0000000002|\n"
            + "|      +--Reorder      |      Reorder|                                                     Order: test.b.f,test.b.g|\n"
            + "|        +--Project    |      Project|                                                  Patterns: test.b.f,test.b.g|\n"
            + "|          +--Select   |       Select|                                                     Filter: test.b.f > 30000|\n"
            + "|            +--Project|      Project|                       Patterns: test.b.f,test.b.g, Target DU: unit0000000002|\n"
            + "+----------------------+-------------+-----------------------------------------------------------------------------+\n"
            + "Total line number = 11\n";
    executor.executeAndCompare(sql6, expect6);
  }

  @Test
  public void testConstantPropagation() {
    if (isFilterPushDown) {
      // 谓词下推规则会把谓词转换成CNF,导致测试结果对比不了，所以这里跳过不测试
      LOGGER.info("Skip SQLSessionIT.testConstantPropagation because filter push down test");
      return;
    }

    String openRule = "SET RULES ConstantPropagationRule=on;";
    String closeRule = "SET RULES ConstantPropagationRule=off;";

    String statement = "EXPLAIN SELECT * FROM us.d1 WHERE %s;";
    List<String> filters =
        Arrays.asList(
            "s1 = 1 and s1 < s2",
            "s1 < s2 and s2 = 3",
            "s2 = 3 and s3 = 0 and s1 < s2 + s3",
            "s2 = 3 or s1 = 4 or s3 < s2",
            "s2 = 3 and s2 < 4",
            "s2 = 3 or (s2 = 2 and s1 < s2)",
            "s1 = 3 and (s1 < 4 or s2 > 5)",
            "s1 = 3 and s1 < 2 and s2 > 5");

    List<String> expectsClosedResult =
        Arrays.asList(
            "us.d1.s1 == 1 && us.d1.s1 < us.d1.s2",
            "us.d1.s1 < us.d1.s2 && us.d1.s2 == 3",
            "us.d1.s2 == 3 && us.d1.s3 == 0 && us.d1.s1 < us.d1.s2 + us.d1.s3",
            "us.d1.s2 == 3 || us.d1.s1 == 4 || us.d1.s3 < us.d1.s2",
            "us.d1.s2 == 3 && us.d1.s2 < 4",
            "us.d1.s2 == 3 || (us.d1.s2 == 2 && us.d1.s1 < us.d1.s2)",
            "us.d1.s1 == 3 && (us.d1.s1 < 4 || us.d1.s2 > 5)",
            "us.d1.s1 == 3 && us.d1.s1 < 2 && us.d1.s2 > 5");

    List<String> expectsOpenedResult =
        Arrays.asList(
            "us.d1.s1 == 1 && us.d1.s2 > 1",
            "us.d1.s1 < 3 && us.d1.s2 == 3",
            "us.d1.s2 == 3 && us.d1.s3 == 0 && us.d1.s1 < 3",
            "us.d1.s2 == 3 || us.d1.s1 == 4 || us.d1.s3 < us.d1.s2",
            "us.d1.s2 == 3",
            "us.d1.s2 == 3 || (us.d1.s2 == 2 && us.d1.s1 < 2)",
            "us.d1.s1 == 3",
            "False");

    executor.execute(closeRule);

    for (int i = 0; i < filters.size(); i++) {
      String result = executor.execute(String.format(statement, filters.get(i)));
      assertTrue(result.contains(expectsClosedResult.get(i)));
    }

    executor.execute(openRule);
    for (int i = 0; i < filters.size(); i++) {
      String result = executor.execute(String.format(statement, filters.get(i)));
      assertTrue(result.contains(expectsOpenedResult.get(i)));
    }
  }

  /** 对常量折叠进行测试，因为RowTransform常量折叠和Filter常量折叠使用的代码都是公共的，所以这里只测试更好对比结果的RowTransform常量折叠 */
  @Test
  public void testConstantFolding() {
    String openRule = "SET RULES RowTransformConstantFoldingRule=on, FilterConstantFoldingRule=on;";
    String closeRule =
        "SET RULES RowTransformConstantFoldingRule=off, FilterConstantFoldingRule=off;";

    executor.execute(openRule);

    // 先是正确性测试，测试常量折叠前后查询结果是否一致
    String statement = "SELECT %s FROM us.d1 LIMIT 1;";
    List<String> openResults = new ArrayList<>();
    List<String> expressions =
        Arrays.asList(
            "411525*s1*s2/4394 + 22680097/13182",
            "((-61/1806 + (81*(-3775*s1/79) + 1377)/(1806*s2)) + 81)",
            "((339152*s1)/35 - s2/35)",
            "s1*(7369/(60*s2))",
            "(60*s1/s2 - 2657/145)",
            "s1/83 + (1/83)*(2623/28)*1/s2 - 58/83",
            "s1/22 + s2/11 + 9/22 - 1/2",
            "((-s2 + (235807*s1/8 - 39)) - 41)",
            "1214/47 + (1/94)*68*1/s2*((322245/s1) + 36)",
            "(-s2 + ((1536697 - 2958*s1) + 93))",
            "53*s1 + 1802*s2 + 161143",
            "(-81*s1 + 1053*s2/59 + 2673/59)",
            "((s2*(27*s1/4 - 2) + 2) + 36)",
            "54*s2*(s1/49 - 67/49) - 4777",
            "((90*(((62*s1/43 + 324198/43)/s2)/74) - 7380) + 1)",
            "-((s1*((15*s2/1274 - 66634/1365) + 3))/14)",
            "1232*(3*(s1/-(660*4/7 + 26400/7))) - 6160",
            "((s2 + 14353/425)/s1)",
            "s2*((51942654/(5609*s1)) + 33)",
            "s2*(-195520 + 85072/(5*s1))",
            "(s1/6072 + s2*(s1 + (s1 + s2*(6076*s2/97 + 4092))/s2)/6072 + 4/253)/s1",
            "s1 + 60*s1*(s2 + 108)/s2 - 60*s2 + 91",
            "(-s1*2*(-s2 + 200*(-2*s2 - 83 - 108*s2/s1))/82 - s1/82 + s2/82 + 10/41)/s2",
            "32*s1/33 + 85*s2*(52*s1*(18*s1*s2*(-s2 - 54) + 18*s2) - 52*s1 + 1352)/33 + s2 + 706/33",
            "s2 - 60 + (149 + (s1 + (-s1 + 3*s2 - 90)/s1)/s1)/s1*2",
            "-s2 + (-s1 + 57*s2 + 57*(-1486*s1/1485 - 8*s2/297 + 79/45 - 41*s2*(-82 + (-s1 + 2*s2)/s1)/(1485*s1))/s1)/s2",
            "(-s1*(-28 + (s1*(s1 + s2*(-s1 - 15 - (-s2 + 9 + s2/s1*2)/(3*s2))) + 32)/s2)/95 - s1 + s2)/s2",
            "-s1*(s2*(-s1 - 47*s2/36 + 47*(s1 + s2 - 490)/(36*s1)) + 86)/1080 - s1/40 - 3*s2/40 - 1/2",
            "-s1 + s2*(-28*s1 - 14*s2 - 14*(-s1*(-2*s1 + 2*s2 - 91)/11 - 12*s1/11 + 287/11)/s2)",
            "s1*(19/90 - (s2*(s1*(-s1*2*s2/83 + 2*s1/83 - 2*s2/83) + s1 + 90) + s2)/(360*s1)) + 8");

    for (String expression : expressions) {
      openResults.add(executor.execute(String.format(statement, expression)));
    }

    executor.execute(closeRule);
    for (int i = 0; i < expressions.size(); i++) {
      String result = executor.execute(String.format(statement, expressions.get(i)));
      // 获取两者第二行第二列的数字
      String openResult = openResults.get(i).split("\n")[4].split("\\|")[2].trim();
      String closeResult = result.split("\n")[4].split("\\|")[2].trim();
      // 转换为double类型进行比较
      double open = Double.parseDouble(openResult);
      double close = Double.parseDouble(closeResult);
      // 误差小于0.00001
      assertEquals(open, close, 0.00001);
    }

    // 下面EXPLAIN一下，测试Filter和RowTransform的常量折叠，还是用上面的语句
    // 这里标注为空字符串的是因为表达式不可折叠，测试时碰到空字符串，会检查是否确实没有折叠（即缺少Rename算子）。
    List<String> foldExpressions =
        Arrays.asList(
            "1720.53535 + 93.65612 × us.d1.s1 × us.d1.s2",
            "80.96622 + 0.00055 × (1377 + -3870.56962 × us.d1.s1) ÷ us.d1.s2",
            "9690.05714 × us.d1.s1 - 0.02857 × us.d1.s2",
            "122.81667 × us.d1.s1 ÷ us.d1.s2",
            "-18.32414 + 60 × us.d1.s1 ÷ us.d1.s2",
            "-0.69880 + 0.01205 × us.d1.s1 + 1.12866 ÷ us.d1.s2",
            "-0.09091 + 0.04545 × us.d1.s1 + 0.09091 × us.d1.s2",
            "-80 + - us.d1.s2 + 29475.87500 × us.d1.s1",
            "25.82979 + 0.72340 ÷ us.d1.s2 × (36 + (322245 ÷ us.d1.s1))",
            "1536790 + - us.d1.s2 - 2958 × us.d1.s1",
            "53 × us.d1.s1 + 1802 × us.d1.s2 + 161143",
            "45.30508 + -81 × us.d1.s1 + 17.84746 × us.d1.s2",
            "38 + us.d1.s2 × (-2 + 6.75000 × us.d1.s1)",
            "-4777 + 54 × us.d1.s2 × (-1.36735 + 0.02041 × us.d1.s1)",
            "-7379 + 1.21622 × (7539.48837 + 1.44186 × us.d1.s1) ÷ us.d1.s2",
            "- (0.07143 × us.d1.s1 × (-45.81612 + 0.01177 × us.d1.s2))",
            "-6160 + -0.89091 × us.d1.s1",
            "((33.77176 + us.d1.s2) ÷ us.d1.s1)",
            "us.d1.s2 × (33 + (9260.59084 ÷ us.d1.s1))",
            "us.d1.s2 × (-195520 + 17014.40000 ÷ us.d1.s1)",
            "(0.01581 + 0.00016 × us.d1.s1 + 0.00016 × us.d1.s2 × (us.d1.s1 + (us.d1.s1 + us.d1.s2 × (4092 + 62.63918 × us.d1.s2)) ÷ us.d1.s2)) ÷ us.d1.s1",
            "us.d1.s1 + 60 × us.d1.s1 × (us.d1.s2 + 108) ÷ us.d1.s2 - 60 × us.d1.s2 + 91",
            "(0.24390 + 0.02439 × - us.d1.s1 × (- us.d1.s2 + 200 × (-83 + -2 × us.d1.s2 - 108 × us.d1.s2 ÷ us.d1.s1)) - 0.01220 × us.d1.s1 + 0.01220 × us.d1.s2) ÷ us.d1.s2",
            "21.39394 + 0.96970 × us.d1.s1 + 2.57576 × us.d1.s2 × (1352 + 52 × us.d1.s1 × (18 × us.d1.s1 × us.d1.s2 × (-54 + - us.d1.s2) + 18 × us.d1.s2) - 52 × us.d1.s1) + us.d1.s2",
            "",
            "- us.d1.s2 + (- us.d1.s1 + 57 × us.d1.s2 + 57 × (1.75556 + -1.00067 × us.d1.s1 - 0.02694 × us.d1.s2 - 0.02761 × us.d1.s2 × (-82 + (- us.d1.s1 + 2 × us.d1.s2) ÷ us.d1.s1) ÷ us.d1.s1) ÷ us.d1.s1) ÷ us.d1.s2",
            "",
            "-0.50000 + 0.00093 × - us.d1.s1 × (86 + us.d1.s2 × (- us.d1.s1 - 1.30556 × us.d1.s2 + 1.30556 × (-490 + us.d1.s1 + us.d1.s2) ÷ us.d1.s1)) - 0.02500 × us.d1.s1 - 0.07500 × us.d1.s2",
            "- us.d1.s1 + us.d1.s2 × (-28 × us.d1.s1 - 14 × us.d1.s2 - 14 × (26.09091 + 0.09091 × - us.d1.s1 × (-91 + -2 × us.d1.s1 + 2 × us.d1.s2) - 1.09091 × us.d1.s1) ÷ us.d1.s2)",
            "8 + us.d1.s1 × (0.21111 - 0.00278 × (us.d1.s2 × (90 + us.d1.s1 × (0.02410 × - us.d1.s1 × us.d1.s2 + 0.02410 × us.d1.s1 - 0.02410 × us.d1.s2) + us.d1.s1) + us.d1.s2) ÷ us.d1.s1)");

    // 先测RowTransform的
    executor.execute(openRule);
    List<String> statements = new ArrayList<>();
    statements.add("EXPLAIN SELECT %s FROM us.d1;");
    statements.add("EXPLAIN SELECT * FROM us.d1 WHERE %s > 0;");
    for (String state : statements) {
      for (int i = 0; i < expressions.size(); i++) {
        String result = executor.execute(String.format(state, expressions.get(i)));
        if (foldExpressions.get(i).isEmpty() || foldExpressions.get(i).equals(expressions.get(i))) {
          assertFalse(result.contains("Rename"));
        } else {
          boolean isContain = result.contains(foldExpressions.get(i));
          if (!isContain) {
            System.out.println(result);
            System.out.println(foldExpressions.get(i));
            fail();
          }
        }
      }
    }
  }

  @Test
  public void testCorrelateSubqueryTime() {
    // 测试IN和EXISTS的关联子查询时间是否相近，在某次修改前EXISTS的时间会比IN的时间长很多（约2个数量级）
    String inQuery =
        "select zipcode, city from us as a \n"
            + "where zipcode not in ( \n"
            + "    SELECT zipcode  \n"
            + "    FROM us \n"
            + "    where us.zipcode = a.zipcode && us.city <> a.city \n"
            + ");";
    String existsQuery =
        "SELECT zipcode, city\n"
            + "FROM us AS a\n"
            + "WHERE NOT EXISTS (\n"
            + "    SELECT *\n"
            + "    FROM us AS b\n"
            + "    WHERE a.zipcode = b.zipcode AND a.city <> b.city\n"
            + ");";

    // 插入数据
    StringBuilder insert = new StringBuilder();
    insert.append("INSERT INTO us (key, zipcode, city) VALUES ");
    int rows = 10000;
    List<String> cityList =
        Arrays.asList(
            "New York",
            "Los Angeles",
            "Chicago",
            "Houston",
            "Phoenix",
            "Philadelphia",
            "San Antonio",
            "San Diego",
            "Dallas");
    for (int i = 0; i < rows; i++) {
      insert.append(
          String.format(
              "(%d, %d, \"%s\")",
              i, (i % 9) + (i % 99) + (i % 999), cityList.get(i % cityList.size())));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    // 检查IN和EXISTS的查询结果是否一致
    String inResult = executor.execute(inQuery);
    String existsResult = executor.execute(existsQuery);
    String expect =
        "ResultSets:\n"
            + "+----+---------+-----------+\n"
            + "| key|a.zipcode|     a.city|\n"
            + "+----+---------+-----------+\n"
            + "|   0|        0|   New York|\n"
            + "|   1|        3|Los Angeles|\n"
            + "|   2|        6|    Chicago|\n"
            + "| 987|     1089|San Antonio|\n"
            + "| 988|     1092|  San Diego|\n"
            + "| 989|     1095|     Dallas|\n"
            + "|9987|     1089|San Antonio|\n"
            + "|9988|     1092|  San Diego|\n"
            + "|9989|     1095|     Dallas|\n"
            + "+----+---------+-----------+\n"
            + "Total line number = 9\n";
    assertEquals(expect, inResult);
    assertEquals(expect, existsResult);

    // explain physical查询
    String inExplainResult = executor.execute("EXPLAIN PHYSICAL " + inQuery);
    String existsExplainResult = executor.execute("EXPLAIN PHYSICAL " + existsQuery);

    // 考虑到波动，两者耗时相差不超过3倍
    int inTime = getTimeCostFromExplainPhysicalResult(inExplainResult);
    int existsTime = getTimeCostFromExplainPhysicalResult(existsExplainResult);
    System.out.println(String.format("IN COST: %dms, EXISTS COST: %dms", inTime, existsTime));
    assertTrue(inTime * 3 >= existsTime || existsTime * 3 >= inTime);
  }

  @Test
  public void testDistinctEliminate() {
    // 插入数据
    StringBuilder insert = new StringBuilder();
    insert.append("INSERT INTO us.d2 (key, s1, s2) VALUES ");
    int rows = 10000;
    for (int i = 0; i < rows; i++) {
      insert.append(String.format("(%d, %d, %d)", i, i % 100, i % 1000));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    String openRule =
        "SET RULES FunctionDistinctEliminateRule=on, InExistsDistinctEliminateRule=on;";
    String closeRule =
        "SET RULES FunctionDistinctEliminateRule=off, InExistsDistinctEliminateRule=off;";

    String closeResult = null;
    // 测试InExistsDistinctEliminateRule
    // 下面两个情况应该会消除Distinct
    String statement = "SELECT * FROM us.d1 WHERE EXISTS (SELECT DISTINCT s1 FROM us.d1);";
    executor.execute(closeRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("Distinct"));
    closeResult = executor.execute(statement);
    executor.execute(openRule);
    assertFalse(executor.execute("EXPLAIN " + statement).contains("Distinct"));
    assertEquals(closeResult, executor.execute(statement));

    statement = "SELECT * FROM us.d1 WHERE s1 IN (SELECT DISTINCT s1 FROM us.d2);";
    executor.execute(closeRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("Distinct"));
    closeResult = executor.execute(statement);
    executor.execute(openRule);
    assertFalse(executor.execute("EXPLAIN " + statement).contains("Distinct"));
    assertEquals(closeResult, executor.execute(statement));

    // 下面情况不会消除Distinct
    statement =
        "SELECT * FROM us.d1 WHERE EXISTS "
            + "(SELECT us.d1.s1, us.d2.s2 FROM us.d1 JOIN (select DISTINCT s1, s2 FROM us.d2) ON us.d1.s1 = us.d2.s1);";
    executor.execute(closeRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("Distinct"));
    closeResult = executor.execute(statement);
    executor.execute(openRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("Distinct"));
    assertEquals(closeResult, executor.execute(statement));

    // 测试FunctionDistinctEliminateRule
    // 下面情况会消除Distinct
    statement = "SELECT max(distinct s1), min(distinct s2) FROM us.d1;";
    executor.execute(closeRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("isDistinct: true"));
    closeResult = executor.execute(statement);
    executor.execute(openRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("isDistinct: false"));
    assertEquals(closeResult, executor.execute(statement));

    statement = "SELECT max(distinct s1) FROM us.d1 GROUP BY s2;";
    executor.execute(closeRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("isDistinct: true"));
    closeResult = executor.execute(statement);
    executor.execute(openRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("isDistinct: false"));
    assertEquals(closeResult, executor.execute(statement));

    // 下面情况不会消除Distinct
    statement = "SELECT max(distinct s1), avg(distinct s2) FROM us.d1;";
    executor.execute(closeRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("isDistinct: true"));
    closeResult = executor.execute(statement);
    executor.execute(openRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("isDistinct: true"));
    assertEquals(closeResult, executor.execute(statement));

    statement = "SELECT avg(distinct s1), count(distinct s2) FROM us.d1 GROUP BY s2, s3;";
    executor.execute(closeRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("isDistinct: true"));
    closeResult = executor.execute(statement);
    executor.execute(openRule);
    assertTrue(executor.execute("EXPLAIN " + statement).contains("isDistinct: true"));
    assertEquals(closeResult, executor.execute(statement));
  }

  @Test
  public void testJoinFactorizationRule() {
    if (isFilterPushDown || isScaling) {
      LOGGER.info(
          "Skip SQLSessionIT.testJoinFactorizationRule because filter push down test or scaling test");
      return;
    }
    String openRule = "SET RULES JoinFactorizationRule=on;";
    String closeRule = "SET RULES JoinFactorizationRule=off;";

    StringBuilder insert = new StringBuilder();
    insert.append("INSERT INTO t1 (key, c1, c2) VALUES ");
    int rows = 50;
    for (int i = 0; i < rows; i++) {
      insert.append(String.format("(%d, %d, %d)", i, i % 10, i % 10));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    insert = new StringBuilder();
    insert.append("INSERT INTO t2 (key, c1, c2) VALUES ");
    for (int i = 0; i < rows; i++) {
      insert.append(String.format("(%d, %d, %d)", i, i % 20, i % 20));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    insert = new StringBuilder();
    insert.append("INSERT INTO t3 (key, c1, c2) VALUES ");
    for (int i = 0; i < rows; i++) {
      insert.append(String.format("(%d, %d, %d)", i, i % 30, i % 30));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    insert = new StringBuilder();
    insert.append("INSERT INTO t4 (key, c1, c2) VALUES ");
    for (int i = 0; i < rows; i++) {
      insert.append(String.format("(%d, %d, %d)", i, i % 40, i % 40));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    // 待优化的语句
    String statement =
        "SELECT t1.c1, t2.c2\n"
            + "   FROM   t1, t2, t3\n"
            + "   WHERE  t1.c1 = t2.c1\n"
            + "   AND    t1.c1 > 1\n"
            + "   AND    t2.c2 = 2\n"
            + "   AND    t2.c2 = t3.c2\n"
            + "   UNION ALL\n"
            + "   SELECT t1.c1, t2.c2\n"
            + "   FROM   t1, t2, t4\n"
            + "   WHERE  t1.c1 = t2.c1\n"
            + "   AND    t1.c1 > 1\n"
            + "   AND    t2.c1 = t4.c1;";
    // 优化后的查询计划与下面这个语句的基本相同,会有些order、filter顺序的不同，不影响结果
    // 例如project t1.c1 t2.c2和project t2.c2 t1.c1的区别
    String optimizing =
        "   SELECT t1.c1, t2.c2\n"
            + "   FROM   t1, (SELECT t2.c1, t2.c2\n"
            + "               FROM   t2, t3\n"
            + "               WHERE  t2.c2 = t3.c2\n"
            + "               AND    t2.c2 = 2\n"
            + "               UNION ALL\n"
            + "               SELECT t2.c1, t2.c2\n"
            + "               FROM   t2, t4\n"
            + "               WHERE  t2.c1 = t4.c1)\n"
            + "   WHERE  t1.c1 = t2.c1\n"
            + "   AND    t1.c1 > 1;";

    executor.execute(openRule);
    String result = executor.execute(statement);
    executor.executeAndCompare(optimizing, result);
    String expect =
        "ResultSets:\n"
            + "+----------------------------+-------------+------------------------------------------------------------------+\n"
            + "|                Logical Tree|Operator Type|                                                     Operator Info|\n"
            + "+----------------------------+-------------+------------------------------------------------------------------+\n"
            + "|Reorder                     |      Reorder|                                                Order: t1.c1,t2.c2|\n"
            + "|  +--Project                |      Project|                                             Patterns: t1.c1,t2.c2|\n"
            + "|    +--Select               |       Select|                             Filter: (t1.c1 == t2.c1 && t1.c1 > 1)|\n"
            + "|      +--CrossJoin          |    CrossJoin|                                        PrefixA: t1, PrefixB: null|\n"
            + "|        +--Project          |      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|        +--Union            |        Union|LeftOrder: t2.c2,t2.c1, RightOrder: t2.c2,t2.c1, isDistinct: false|\n"
            + "|          +--Reorder        |      Reorder|                                                Order: t2.c1,t2.c2|\n"
            + "|            +--Project      |      Project|                                             Patterns: t2.c1,t2.c2|\n"
            + "|              +--Select     |       Select|                                Filter: (t2.c2 == 2 && t3.c2 == 2)|\n"
            + "|                +--CrossJoin|    CrossJoin|                                          PrefixA: t2, PrefixB: t3|\n"
            + "|                  +--Project|      Project|                  Patterns: t2.c1,t2.c2, Target DU: unit0000000002|\n"
            + "|                  +--Project|      Project|                        Patterns: t3.c2, Target DU: unit0000000002|\n"
            + "|          +--Reorder        |      Reorder|                                                Order: t2.c1,t2.c2|\n"
            + "|            +--Project      |      Project|                                             Patterns: t2.c1,t2.c2|\n"
            + "|              +--Select     |       Select|                                          Filter: (t2.c1 == t4.c1)|\n"
            + "|                +--CrossJoin|    CrossJoin|                                          PrefixA: t2, PrefixB: t4|\n"
            + "|                  +--Project|      Project|                  Patterns: t2.c1,t2.c2, Target DU: unit0000000002|\n"
            + "|                  +--Project|      Project|                        Patterns: t4.c1, Target DU: unit0000000002|\n"
            + "+----------------------------+-------------+------------------------------------------------------------------+\n"
            + "Total line number = 18\n";
    assertEquals(expect, executor.execute("EXPLAIN " + statement));

    executor.execute(closeRule);
    assertTrue(TestUtils.compareTables(executor.execute(optimizing), result));
    expect =
        "ResultSets:\n"
            + "+----------------------+-------------+------------------------------------------------------------------+\n"
            + "|          Logical Tree|Operator Type|                                                     Operator Info|\n"
            + "+----------------------+-------------+------------------------------------------------------------------+\n"
            + "|Union                 |        Union|LeftOrder: t1.c1,t2.c2, RightOrder: t1.c1,t2.c2, isDistinct: false|\n"
            + "|  +--Reorder          |      Reorder|                                                Order: t1.c1,t2.c2|\n"
            + "|    +--Project        |      Project|                                             Patterns: t2.c2,t1.c1|\n"
            + "|      +--Select       |       Select| Filter: (t1.c1 == t2.c1 && t1.c1 > 1 && t2.c2 == 2 && t3.c2 == 2)|\n"
            + "|        +--CrossJoin  |    CrossJoin|                                          PrefixA: t2, PrefixB: t3|\n"
            + "|          +--CrossJoin|    CrossJoin|                                          PrefixA: t1, PrefixB: t2|\n"
            + "|            +--Project|      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|            +--Project|      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|          +--Project  |      Project|                         Patterns: t3.*, Target DU: unit0000000002|\n"
            + "|  +--Reorder          |      Reorder|                                                Order: t1.c1,t2.c2|\n"
            + "|    +--Project        |      Project|                                             Patterns: t2.c2,t1.c1|\n"
            + "|      +--Select       |       Select|           Filter: (t1.c1 == t2.c1 && t1.c1 > 1 && t2.c1 == t4.c1)|\n"
            + "|        +--CrossJoin  |    CrossJoin|                                          PrefixA: t2, PrefixB: t4|\n"
            + "|          +--CrossJoin|    CrossJoin|                                          PrefixA: t1, PrefixB: t2|\n"
            + "|            +--Project|      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|            +--Project|      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|          +--Project  |      Project|                         Patterns: t4.*, Target DU: unit0000000002|\n"
            + "+----------------------+-------------+------------------------------------------------------------------+\n"
            + "Total line number = 17\n";
    assertEquals(expect, executor.execute("EXPLAIN " + statement));

    statement =
        "SELECT t1.c2, t2.c2\n"
            + "FROM   t1, t2\n"
            + "WHERE  t1.c1 = t2.c1 \n"
            + "AND    t1.c1 = 1\n"
            + "UNION ALL\n"
            + "SELECT t1.c2, t2.c2\n"
            + "FROM   t1, t2\n"
            + "WHERE  t1.c1 = t2.c1 \n"
            + "AND    t1.c1 = 2;";
    optimizing =
        "SELECT t1.c2, t2.c2\n"
            + "FROM   t2, (SELECT c1, c2\n"
            + "            FROM   t1\n"
            + "            WHERE  t1.c1 = 1\n"
            + "            UNION ALL\n"
            + "            SELECT c1, c2\n"
            + "            FROM   t1\n"
            + "            WHERE  t1.c1 = 2)\n"
            + "WHERE  t1.c1 = t2.c1;";

    executor.execute(openRule);
    result = executor.execute(statement);
    executor.executeAndCompare(optimizing, result);
    expect =
        "ResultSets:\n"
            + "+--------------------------+-------------+------------------------------------------------------------------+\n"
            + "|              Logical Tree|Operator Type|                                                     Operator Info|\n"
            + "+--------------------------+-------------+------------------------------------------------------------------+\n"
            + "|Reorder                   |      Reorder|                                                Order: t1.c2,t2.c2|\n"
            + "|  +--Project              |      Project|                                             Patterns: t1.c2,t2.c2|\n"
            + "|    +--Select             |       Select|                                          Filter: (t1.c1 == t2.c1)|\n"
            + "|      +--CrossJoin        |    CrossJoin|                                        PrefixA: t2, PrefixB: null|\n"
            + "|        +--Project        |      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|        +--Union          |        Union|LeftOrder: t1.c2,t1.c1, RightOrder: t1.c2,t1.c1, isDistinct: false|\n"
            + "|          +--Reorder      |      Reorder|                                                Order: t1.c1,t1.c2|\n"
            + "|            +--Project    |      Project|                                             Patterns: t1.c1,t1.c2|\n"
            + "|              +--Select   |       Select|                                              Filter: (t1.c1 == 1)|\n"
            + "|                +--Project|      Project|                  Patterns: t1.c1,t1.c2, Target DU: unit0000000002|\n"
            + "|          +--Reorder      |      Reorder|                                                Order: t1.c1,t1.c2|\n"
            + "|            +--Project    |      Project|                                             Patterns: t1.c1,t1.c2|\n"
            + "|              +--Select   |       Select|                                              Filter: (t1.c1 == 2)|\n"
            + "|                +--Project|      Project|                  Patterns: t1.c1,t1.c2, Target DU: unit0000000002|\n"
            + "+--------------------------+-------------+------------------------------------------------------------------+\n"
            + "Total line number = 14\n";
    assertEquals(expect, executor.execute("EXPLAIN " + statement));

    executor.execute(closeRule);
    assertTrue(TestUtils.compareTables(executor.execute(optimizing), result));
    expect =
        "ResultSets:\n"
            + "+--------------------+-------------+------------------------------------------------------------------+\n"
            + "|        Logical Tree|Operator Type|                                                     Operator Info|\n"
            + "+--------------------+-------------+------------------------------------------------------------------+\n"
            + "|Union               |        Union|LeftOrder: t1.c2,t2.c2, RightOrder: t1.c2,t2.c2, isDistinct: false|\n"
            + "|  +--Reorder        |      Reorder|                                                Order: t1.c2,t2.c2|\n"
            + "|    +--Project      |      Project|                                             Patterns: t2.c2,t1.c2|\n"
            + "|      +--Select     |       Select|                                Filter: (t2.c1 == 1 && t1.c1 == 1)|\n"
            + "|        +--CrossJoin|    CrossJoin|                                          PrefixA: t1, PrefixB: t2|\n"
            + "|          +--Project|      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|          +--Project|      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|  +--Reorder        |      Reorder|                                                Order: t1.c2,t2.c2|\n"
            + "|    +--Project      |      Project|                                             Patterns: t2.c2,t1.c2|\n"
            + "|      +--Select     |       Select|                                Filter: (t2.c1 == 2 && t1.c1 == 2)|\n"
            + "|        +--CrossJoin|    CrossJoin|                                          PrefixA: t1, PrefixB: t2|\n"
            + "|          +--Project|      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|          +--Project|      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "+--------------------+-------------+------------------------------------------------------------------+\n"
            + "Total line number = 13\n";
    assertEquals(expect, executor.execute("EXPLAIN " + statement));
  }

  @Test
  public void testOuterJoinEliminate() {
    StringBuilder insert = new StringBuilder();
    insert.append("INSERT INTO us (key, d2.s1, d2.s2, d3.s1, d3.s2) VALUES ");
    int rows = 15000;
    for (int i = 0; i < rows; i++) {
      insert.append(String.format("(%d, %d, %d, %d, %d)", i, i % 100, i % 150, i % 200, i % 250));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    String openRule = "SET RULES OuterJoinEliminateRule=on;";
    String closeRule = "SET RULES OuterJoinEliminateRule=off;";
    String openResult, openExplain, closeResult, closeExplain;

    String statement =
        "SELECT distinct us.d1.s1, us.d1.s2 FROM us.d1 LEFT JOIN us.d2 ON us.d1.s1 = us.d2.s1;";
    executor.execute(openRule);
    openResult = executor.execute(statement);
    openExplain = executor.execute("EXPLAIN " + statement);
    executor.execute(closeRule);
    closeResult = executor.execute(statement);
    closeExplain = executor.execute("EXPLAIN " + statement);
    assertEquals(openResult, closeResult);
    assertTrue(!openExplain.contains("OuterJoin") && closeExplain.contains("OuterJoin"));

    statement =
        "SELECT * FROM us.d1 WHERE s1 IN (SELECT us.d2.s1 FROM us.d2 LEFT JOIN us.d3 ON us.d2.s1 = us.d3.s1);";
    executor.execute(openRule);
    openResult = executor.execute(statement);
    openExplain = executor.execute("EXPLAIN " + statement);
    executor.execute(closeRule);
    closeResult = executor.execute(statement);
    closeExplain = executor.execute("EXPLAIN " + statement);
    assertEquals(openResult, closeResult);
    assertTrue(!openExplain.contains("OuterJoin") && closeExplain.contains("OuterJoin"));

    statement =
        "SELECT * FROM us.d1 WHERE EXISTS "
            + "(SELECT us.d2.s1 FROM us.d2 LEFT JOIN us.d3 ON us.d2.s1 = us.d3.s1);";
    executor.execute(openRule);
    openResult = executor.execute(statement);
    openExplain = executor.execute("EXPLAIN " + statement);
    executor.execute(closeRule);
    closeResult = executor.execute(statement);
    closeExplain = executor.execute("EXPLAIN " + statement);
    assertEquals(openResult, closeResult);
    assertTrue(!openExplain.contains("OuterJoin") && closeExplain.contains("OuterJoin"));
  }
}
