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
package cn.edu.tsinghua.iginx.integration.func.optimizer;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.*;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizerIT {
  protected static SQLExecutor executor;

  protected static boolean isForSession = true, isForSessionPool = false;
  protected static int MaxMultiThreadTaskNum = -1;

  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";
  protected static String runningEngine;

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionIT.class);

  protected boolean isAbleToDelete;

  protected boolean isSupportChinesePath;

  protected boolean isSupportNumericalPath;

  protected boolean isSupportSpecialCharacterPath;

  protected boolean isAbleToShowColumns;

  protected boolean isScaling = false;

  private final long startKey = 0L;

  private final long endKey = 15000L;

  private boolean isOptimizerOpen;

  protected boolean isAbleToClearData;
  private static MultiConnection session;

  private static boolean dummyNoData = true;

  protected static boolean needCompareResult = true;

  private List<String> ruleList = new ArrayList<>();

  public OptimizerIT() {
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
    this.isSupportChinesePath = dbConf.getEnumValue(DBConf.DBConfType.isSupportChinesePath);
    this.isSupportNumericalPath = dbConf.getEnumValue(DBConf.DBConfType.isSupportNumericalPath);
    this.isSupportSpecialCharacterPath =
        dbConf.getEnumValue(DBConf.DBConfType.isSupportSpecialCharacterPath);

    String rules = executor.execute("SHOW RULES;");
    this.isOptimizerOpen = rules.contains("FilterPushDownRule|    ON|");
    if (isOptimizerOpen) {
      ruleList = getRuleList();
    }
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

  private static List<String> getRuleList() {
    String ruleTable = executor.execute("SHOW RULES;");
    Pattern pattern = Pattern.compile("\\|\\s+([A-Za-z]+Rule)\\|");
    Matcher matcher = pattern.matcher(ruleTable);
    List<String> ruleList = new ArrayList<>();
    while (matcher.find()) {
      ruleList.add(matcher.group(1));
    }
    return ruleList;
  }

  @Before
  public void closeAllRules() {
    StringBuilder sb = new StringBuilder();
    for (String rule : ruleList) {
      sb.append(" ").append(rule).append("=off,");
    }
    String statement = "set rules" + sb.substring(0, sb.length() - 1) + ";";
    executor.execute(statement);
  }

  @After
  public void openAllRules() {
    StringBuilder sb = new StringBuilder();
    for (String rule : ruleList) {
      sb.append(" ").append(rule).append("=on,");
    }
    String statement = "set rules" + sb.substring(0, sb.length() - 1) + ";";
    executor.execute(statement);
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

  @After
  public void clearData() {
    String clearData = "CLEAR DATA;";
    executor.execute(clearData);
  }

  @Test
  public void testModifyRules() {
    if (!isOptimizerOpen) {
      LOGGER.info("Skip SQLSessionIT.testModifyRules because optimizer is not open");
      return;
    }

    String statement, expected;
    statement = "show rules;";

    expected = "FragmentPruningByFilterRule|   OFF";

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
    if (!isOptimizerOpen) {
      LOGGER.info("Skip SQLSessionIT.testFilterPushDownExplain because optimizer is not open");
      return;
    }

    executor.execute("SET RULES FilterPushDownRule=on;");

    String insert =
        "INSERT INTO us.d2(key, c) VALUES (1, \"asdas\"), (2, \"sadaa\"), (3, \"sadada\"), (4, \"asdad\"), (5, \"deadsa\"), (6, \"dasda\"), (7, \"asdsad\"), (8, \"frgsa\"), (9, \"asdad\");";
    executor.execute(insert);
    insert =
        "INSERT INTO us.d3(key, s1) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9);";
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
                    + "|        +--GroupBy    |      GroupBy|GroupByCols: us.d1.s1, FuncList(Name, FuncType): (max, System),(min, System), MappingType: SetMapping isDistinct: false|\n"
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
  }

  @Test
  public void testFilterFragmentOptimizer() {
    String policy = executor.execute("SHOW CONFIG \"policyClassName\";");
    if (!policy.contains("KeyRangeTestPolicy")) {
      LOGGER.info(
          "Skip SQLSessionIT.testFilterFragmentOptimizer because policy is not KeyRangeTestPolicy");
      return;
    }

    if (!isOptimizerOpen) {
      LOGGER.info(
          "Skip SQLSessionIT.testFilterFragmentOptimizer because optimizer is not remove_not,filter_fragment");
      return;
    }

    if (isScaling) {
      LOGGER.info("Skip SQLSessionIT.testFilterFragmentOptimizer because it is scaling test");
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
                    + "|      +--Rename         |       Rename|                                                             AliasList: (us.d2.a, aa),(us.d2.b, bb)|\n"
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
                    + "|      +--Rename         |       Rename|                                                                                    AliasList: (avg(us.d1.s1), avg_s1),(sum(us.d1.s2), sum_s2)|\n"
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
                    + "|      +--Rename           |       Rename|                                                                                    AliasList: (avg(us.d1.s1), avg_s1),(sum(us.d1.s2), sum_s2)|\n"
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
  }

  @Test
  public void testColumnPruningAndFragmentPruning() {
    if (!isOptimizerOpen || isScaling) {
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

    String openRule = "SET RULES ColumnPruningRule=ON;";
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
    if (!isOptimizerOpen) {
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
    if (!isOptimizerOpen) {
      LOGGER.info("Skip SQLSessionIT.testConstantFolding because optimizer is closed");
      return;
    }
    String openRule = "SET RULES ConstantFoldingRule=on;";
    String closeRule = "SET RULES ConstantFoldingRule=off;";

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
  public void testDistinctEliminate() {
    if (!isOptimizerOpen) {
      LOGGER.info("Skip SQLSessionIT.testDistinctEliminate because optimizer is closed");
      return;
    }
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

    String openRule = "SET RULES DistinctEliminateRule=on;";
    String closeRule = "SET RULES DistinctEliminateRule=off;";

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
    if (!isOptimizerOpen || isScaling) {
      LOGGER.info(
          "Skip SQLSessionIT.testJoinFactorizationRule because optimizer is closed or scaling test");
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
            + "|          +--Reorder        |      Reorder|                                                Order: t2.c2,t2.c1|\n"
            + "|            +--Project      |      Project|                                             Patterns: t2.c2,t2.c1|\n"
            + "|              +--Select     |       Select|                            Filter: (t2.c2 == 2 && t2.c2 == t3.c2)|\n"
            + "|                +--CrossJoin|    CrossJoin|                                          PrefixA: t2, PrefixB: t3|\n"
            + "|                  +--Project|      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|                  +--Project|      Project|                         Patterns: t3.*, Target DU: unit0000000002|\n"
            + "|          +--Reorder        |      Reorder|                                                Order: t2.c2,t2.c1|\n"
            + "|            +--Project      |      Project|                                             Patterns: t2.c2,t2.c1|\n"
            + "|              +--Select     |       Select|                                          Filter: (t2.c1 == t4.c1)|\n"
            + "|                +--CrossJoin|    CrossJoin|                                          PrefixA: t2, PrefixB: t4|\n"
            + "|                  +--Project|      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|                  +--Project|      Project|                         Patterns: t4.*, Target DU: unit0000000002|\n"
            + "+----------------------------+-------------+------------------------------------------------------------------+\n"
            + "Total line number = 18\n";
    assertEquals(expect, executor.execute("EXPLAIN " + statement));

    executor.execute(closeRule);
    assertTrue(TestUtils.compareTables(executor.execute(optimizing), result));
    expect =
        "ResultSets:\n"
            + "+----------------------+-------------+---------------------------------------------------------------------+\n"
            + "|          Logical Tree|Operator Type|                                                        Operator Info|\n"
            + "+----------------------+-------------+---------------------------------------------------------------------+\n"
            + "|Union                 |        Union|   LeftOrder: t1.c1,t2.c2, RightOrder: t1.c1,t2.c2, isDistinct: false|\n"
            + "|  +--Reorder          |      Reorder|                                                   Order: t1.c1,t2.c2|\n"
            + "|    +--Project        |      Project|                                                Patterns: t2.c2,t1.c1|\n"
            + "|      +--Select       |       Select|Filter: (t1.c1 == t2.c1 && t1.c1 > 1 && t2.c2 == 2 && t2.c2 == t3.c2)|\n"
            + "|        +--CrossJoin  |    CrossJoin|                                             PrefixA: t2, PrefixB: t3|\n"
            + "|          +--CrossJoin|    CrossJoin|                                             PrefixA: t1, PrefixB: t2|\n"
            + "|            +--Project|      Project|                            Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|            +--Project|      Project|                            Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|          +--Project  |      Project|                            Patterns: t3.*, Target DU: unit0000000002|\n"
            + "|  +--Reorder          |      Reorder|                                                   Order: t1.c1,t2.c2|\n"
            + "|    +--Project        |      Project|                                                Patterns: t2.c2,t1.c1|\n"
            + "|      +--Select       |       Select|              Filter: (t1.c1 == t2.c1 && t1.c1 > 1 && t2.c1 == t4.c1)|\n"
            + "|        +--CrossJoin  |    CrossJoin|                                             PrefixA: t2, PrefixB: t4|\n"
            + "|          +--CrossJoin|    CrossJoin|                                             PrefixA: t1, PrefixB: t2|\n"
            + "|            +--Project|      Project|                            Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|            +--Project|      Project|                            Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|          +--Project  |      Project|                            Patterns: t4.*, Target DU: unit0000000002|\n"
            + "+----------------------+-------------+---------------------------------------------------------------------+\n"
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
            + "|          +--Reorder      |      Reorder|                                                Order: t1.c2,t1.c1|\n"
            + "|            +--Project    |      Project|                                             Patterns: t1.c2,t1.c1|\n"
            + "|              +--Select   |       Select|                                              Filter: (t1.c1 == 1)|\n"
            + "|                +--Project|      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|          +--Reorder      |      Reorder|                                                Order: t1.c2,t1.c1|\n"
            + "|            +--Project    |      Project|                                             Patterns: t1.c2,t1.c1|\n"
            + "|              +--Select   |       Select|                                              Filter: (t1.c1 == 2)|\n"
            + "|                +--Project|      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
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
            + "|      +--Select     |       Select|                            Filter: (t1.c1 == t2.c1 && t1.c1 == 1)|\n"
            + "|        +--CrossJoin|    CrossJoin|                                          PrefixA: t1, PrefixB: t2|\n"
            + "|          +--Project|      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|          +--Project|      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "|  +--Reorder        |      Reorder|                                                Order: t1.c2,t2.c2|\n"
            + "|    +--Project      |      Project|                                             Patterns: t2.c2,t1.c2|\n"
            + "|      +--Select     |       Select|                            Filter: (t1.c1 == t2.c1 && t1.c1 == 2)|\n"
            + "|        +--CrossJoin|    CrossJoin|                                          PrefixA: t1, PrefixB: t2|\n"
            + "|          +--Project|      Project|                         Patterns: t1.*, Target DU: unit0000000002|\n"
            + "|          +--Project|      Project|                         Patterns: t2.*, Target DU: unit0000000002|\n"
            + "+--------------------+-------------+------------------------------------------------------------------+\n"
            + "Total line number = 13\n";
    assertEquals(expect, executor.execute("EXPLAIN " + statement));
  }

  @Test
  public void testOuterJoinEliminate() {
    if (!isOptimizerOpen) {
      LOGGER.info("Skip SQLSessionIT.testOuterJoinEliminate because optimizer is closed");
      return;
    }

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

  @Test
  public void testInFilterTransformRule() {
    if (!isOptimizerOpen) {
      LOGGER.info("Skip SQLSessionIT.testInFilterTransformRule because optimizer is closed");
      return;
    }

    // 插入数据
    StringBuilder insert = new StringBuilder();
    insert.append("INSERT INTO us.d2 (key, s1, s2) VALUES ");
    int rows = 1000;
    for (int i = 0; i < rows; i++) {
      insert.append(String.format("(%d, %d, %d)", i, i % 100, i % 1000));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    String openRule = "SET RULES InFilterTransformRule=on;";
    String closeRule = "SET RULES InFilterTransformRule=off;";

    String statement, openRes, closeRes, openExplain, closeExplain;
    statement = "SELECT s1,s2 FROM us.d1 WHERE s1 = 1 OR s1 = 2 OR s1 = 3;";
    executor.execute(openRule);
    openRes = executor.execute(statement);
    openExplain = executor.execute("EXPLAIN " + statement);
    executor.execute(closeRule);
    closeRes = executor.execute(statement);
    closeExplain = executor.execute("EXPLAIN " + statement);

    assertEquals(openRes, closeRes);
    // 由于in filter使用的Hashset每次输出元素的顺序不固定，所以只能判断是否包含
    assertTrue(openExplain.contains("us.d1.s1 in"));
    assertTrue(closeExplain.contains("us.d1.s1 == 1 || us.d1.s1 == 2 || us.d1.s1 == 3"));

    statement = "SELECT s1 FROM us.* WHERE s1 &= 1 OR s1 &= 2 OR s1 |= 3 OR s1 |= 4;";
    executor.execute(openRule);
    openRes = executor.execute(statement);
    openExplain = executor.execute("EXPLAIN " + statement);
    executor.execute(closeRule);
    closeRes = executor.execute(statement);
    closeExplain = executor.execute("EXPLAIN " + statement);

    assertEquals(openRes, closeRes);
    assertTrue(!openExplain.contains("us.*.s1 &in") && openExplain.contains("us.*.s1 in"));
    assertTrue(
        closeExplain.contains("us.*.s1 &== 1 || us.*.s1 &== 2 || us.*.s1 == 3 || us.*.s1 == 4"));

    statement = "SELECT s1,s2 FROM us.d1 WHERE s1 != 1 AND s1 != 2 AND s1 != 3;";
    executor.execute(openRule);
    openRes = executor.execute(statement);
    openExplain = executor.execute("EXPLAIN " + statement);
    executor.execute(closeRule);
    closeRes = executor.execute(statement);
    closeExplain = executor.execute("EXPLAIN " + statement);

    assertEquals(openRes, closeRes);
    assertTrue(openExplain.contains("us.d1.s1 &not in"));
    assertTrue(closeExplain.contains("us.d1.s1 != 1 && us.d1.s1 != 2 && us.d1.s1 != 3"));

    statement = "SELECT s1 FROM us.* WHERE s1 &!= 1 AND s1 &!= 2 AND s1 |!= 3 AND s1 |!= 4;";
    executor.execute(openRule);
    openRes = executor.execute(statement);
    openExplain = executor.execute("EXPLAIN " + statement);
    executor.execute(closeRule);
    closeRes = executor.execute(statement);
    closeExplain = executor.execute("EXPLAIN " + statement);

    assertEquals(openRes, closeRes);
    assertTrue(openExplain.contains("us.*.s1 &not in") && !openExplain.contains("us.*.s1 not in"));
    assertTrue(
        closeExplain.contains("us.*.s1 &!= 1 && us.*.s1 &!= 2 && us.*.s1 != 3 && us.*.s1 != 4"));
  }
}
