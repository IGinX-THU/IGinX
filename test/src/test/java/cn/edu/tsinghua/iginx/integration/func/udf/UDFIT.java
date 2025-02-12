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
package cn.edu.tsinghua.iginx.integration.func.udf;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskInfo;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import cn.edu.tsinghua.iginx.utils.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(UDFIT.class);

  private static final double delta = 0.01d;

  private static boolean isScaling;

  private static Session session;

  private static boolean dummyNoData = true;

  private static boolean needCompareResult = true;

  private static List<String> taskToBeRemoved;

  private static final String SINGLE_UDF_REGISTER_SQL =
      "CREATE FUNCTION %s \"%s\" FROM \"%s\" IN \"%s\";";

  private static final String MULTI_UDF_REGISTER_SQL = "CREATE FUNCTION %s IN \"%s\";";

  private static final String DROP_SQL = "DROP FUNCTION \"%s\";";

  private static final String SHOW_FUNCTION_SQL = "SHOW FUNCTIONS;";

  private static final String MODULE_PATH =
      String.join(
          File.separator,
          System.getProperty("user.dir"),
          "src",
          "test",
          "resources",
          "udf",
          "my_module");

  private static final String MODULE_FILE_PATH =
      String.join(
          File.separator,
          System.getProperty("user.dir"),
          "src",
          "test",
          "resources",
          "udf",
          "my_module",
          "idle_classes.py");

  private static UDFTestTools tool;

  @BeforeClass
  public static void setUp() throws SessionException {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    isScaling = conf.isScaling();
    if (!SUPPORT_KEY.get(conf.getStorageType()) && conf.isScaling()) {
      needCompareResult = false;
    }
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
    tool = new UDFTestTools(session);
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    session.closeSession();
  }

  @Before
  public void insertData() {
    long startKey = 0L;
    long endKey = 15000L;
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
    int size = (int) (endKey - startKey);
    for (int i = 0; i < size; i++) {
      keyList.add(startKey + i);
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
    Controller.after(session);
  }

  @After
  public void clearData() {
    Controller.clearData(session);
  }

  @Before
  public void resetTaskToBeDropped() {
    if (taskToBeRemoved == null) {
      taskToBeRemoved = new ArrayList<>();
    } else {
      taskToBeRemoved.clear();
    }
  }

  // drop unwanted UDFs no matter what
  @After
  public void dropTasks() {
    tool.dropTasks(taskToBeRemoved);
    taskToBeRemoved.clear();
  }

  @Test
  public void baseTests() {
    String udtfSQLFormat = "SELECT %s(s1) FROM us.d1 WHERE key < 200;";
    String udafSQLFormat = "SELECT %s(s1) FROM us.d1 OVER WINDOW (size 50 IN [0, 200));";
    String udsfSQLFormat = "SELECT %s(s1) FROM us.d1 WHERE key < 50;";

    SessionExecuteSqlResult ret = tool.execute(SHOW_FUNCTION_SQL);

    List<RegisterTaskInfo> registerUDFs = ret.getRegisterTaskInfos();
    for (RegisterTaskInfo info : registerUDFs) {
      // execute udf
      if (info.getType().equals(UDFType.UDTF)) {
        tool.execute(String.format(udtfSQLFormat, info.getName()));
      } else if (info.getType().equals(UDFType.UDAF)) {
        tool.execute(String.format(udafSQLFormat, info.getName()));
      } else if (info.getType().equals(UDFType.UDSF)) {
        tool.execute(String.format(udsfSQLFormat, info.getName()));
      }
    }
  }

  @Test
  public void testDropTask() {
    String filePath =
        String.join(
            File.separator,
            System.getProperty("user.dir"),
            "src",
            "test",
            "resources",
            "udf",
            "mock_udf.py");
    String udfName = "mock_udf";
    tool.executeReg(String.format(SINGLE_UDF_REGISTER_SQL, "UDAF", udfName, "MockUDF", filePath));
    assertTrue(tool.isUDFRegistered(udfName));
    taskToBeRemoved.add(udfName);

    tool.execute(String.format(DROP_SQL, udfName));
    try {
      Thread.sleep(1000); // needed in some tests(redis no+no yes+no)
    } catch (InterruptedException e) {
      LOGGER.error("Thread sleep error.", e);
    }
    // dropped udf cannot be queried
    assertFalse(tool.isUDFRegistered(udfName));
    taskToBeRemoved.clear();

    // make sure dropped udf cannot be used
    String statement = "SELECT " + udfName + "(s1,1) FROM us.d1 WHERE s1 < 10;";
    tool.executeFail(statement);
  }

  @Test
  public void testCOS() {
    String statement = "SELECT cos(s1) FROM us.d1 WHERE s1 < 10;";

    SessionExecuteSqlResult ret = tool.execute(statement);
    compareResult(Collections.singletonList("cos(us.d1.s1)"), ret.getPaths());
    compareResult(new long[] {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L}, ret.getKeys());

    List<Double> expectedValues =
        Arrays.asList(
            1.0,
            0.5403023058681398,
            -0.4161468365471424,
            -0.9899924966004454,
            -0.6536436208636119,
            0.2836621854632263,
            0.9601702866503661,
            0.7539022543433046,
            -0.14550003380861354,
            -0.9111302618846769);
    for (int i = 0; i < ret.getValues().size(); i++) {
      compareResult(1, ret.getValues().get(i).size());
      double expected = expectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      compareResult(expected, actual, delta);
    }
  }

  @Test
  public void testNestedUDF() {
    String statement = "SELECT arccos(cos(s1)) FROM us.d1 WHERE s1 < 4;";
    SessionExecuteSqlResult ret = tool.execute(statement);
    compareResult(Collections.singletonList("arccos(cos(us.d1.s1))"), ret.getPaths());
    compareResult(new long[] {0L, 1L, 2L, 3L}, ret.getKeys());
    List<Double> expectedValues = Arrays.asList(0.0, 1.0, 2.0, 3.0);
    for (int i = 0; i < ret.getValues().size(); i++) {
      compareResult(1, ret.getValues().get(i).size());
      double expected = expectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      compareResult(expected, actual, delta);
    }

    statement = "SELECT sum(cos(s1)), avg(cos(s1)) FROM us.d1 WHERE s1 < 10;";
    ret = tool.execute(statement);
    compareResult(Arrays.asList("sum(cos(us.d1.s1))", "avg(cos(us.d1.s1))"), ret.getPaths());
    assertEquals(1, ret.getValues().size());
    expectedValues = Arrays.asList(0.42162378262054656, 0.042162378262054656);
    assertEquals(2, ret.getValues().get(0).size());
    for (int i = 0; i < 2; i++) {
      double expected = expectedValues.get(i);
      double actual = (double) ret.getValues().get(0).get(i);
      compareResult(expected, actual, delta);
    }

    statement = "SELECT avg(s1), cos(avg(s1)) FROM us.d1 WHERE s1 < 10;";
    ret = tool.execute(statement);
    compareResult(Arrays.asList("avg(us.d1.s1)", "cos(avg(us.d1.s1))"), ret.getPaths());
    assertEquals(1, ret.getValues().size());
    expectedValues = Arrays.asList(4.5, -0.2107957994307797);
    assertEquals(2, ret.getValues().get(0).size());
    for (int i = 0; i < 2; i++) {
      double expected = expectedValues.get(i);
      double actual = (double) ret.getValues().get(0).get(i);
      compareResult(expected, actual, delta);
    }
  }

  @Test
  public void testConcurrentCos() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    tool.execute(insert);

    String query =
        "SELECT * FROM (SELECT cos(s1) AS cos_s1 FROM test) AS t1, (SELECT cos(s2) AS cos_s2 FROM test) AS t2 LIMIT 10;";
    SessionExecuteSqlResult ret = tool.execute(query);
    compareResult(4, ret.getPaths().size());

    List<Double> cosS1ExpectedValues =
        Arrays.asList(
            -0.4161468365471424,
            -0.4161468365471424,
            -0.4161468365471424,
            -0.4161468365471424,
            -0.4161468365471424,
            -0.4161468365471424,
            -0.9899924966004454,
            -0.9899924966004454,
            -0.9899924966004454,
            -0.9899924966004454);
    List<Double> cosS2ExpectedValues =
        Arrays.asList(
            -0.9899924966004454,
            0.5403023058681398,
            -0.9899924966004454,
            0.7539022543433046,
            0.9601702866503661,
            -0.6536436208636119,
            -0.9899924966004454,
            0.5403023058681398,
            -0.9899924966004454,
            0.7539022543433046);

    for (int i = 0; i < ret.getValues().size(); i++) {
      compareResult(4, ret.getValues().get(i).size());
      double expected = cosS1ExpectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      compareResult(expected, actual, delta);

      expected = cosS2ExpectedValues.get(i);
      actual = (double) ret.getValues().get(i).get(2);
      compareResult(expected, actual, delta);
    }
  }

  @Test
  public void testMultiColumns() {
    String insert =
        "INSERT INTO test(key, s1, s2, s3) VALUES (1, 2, 3, 2), (2, 3, 1, 3), (3, 4, 3, 1), (4, 9, 7, 5), (5, 3, 6, 2), (6, 6, 4, 2);";
    tool.execute(insert);

    String query = "SELECT multiply(s1, s2) FROM test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+--------------------------+\n"
            + "|key|multiply(test.s1, test.s2)|\n"
            + "+---+--------------------------+\n"
            + "|  1|                       6.0|\n"
            + "|  2|                       3.0|\n"
            + "|  3|                      12.0|\n"
            + "|  4|                      63.0|\n"
            + "|  5|                      18.0|\n"
            + "|  6|                      24.0|\n"
            + "+---+--------------------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT multiply(s1, s2, s3) FROM test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-----------------------------------+\n"
            + "|key|multiply(test.s1, test.s2, test.s3)|\n"
            + "+---+-----------------------------------+\n"
            + "|  1|                               12.0|\n"
            + "|  2|                                9.0|\n"
            + "|  3|                               12.0|\n"
            + "|  4|                              315.0|\n"
            + "|  5|                               36.0|\n"
            + "|  6|                               48.0|\n"
            + "+---+-----------------------------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT multiply(s1, s2, s3), s1, s2 + s3 FROM test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-----------------------------------+-------+-----------------+\n"
            + "|key|multiply(test.s1, test.s2, test.s3)|test.s1|test.s2 + test.s3|\n"
            + "+---+-----------------------------------+-------+-----------------+\n"
            + "|  1|                               12.0|      2|                5|\n"
            + "|  2|                                9.0|      3|                4|\n"
            + "|  3|                               12.0|      4|                4|\n"
            + "|  4|                              315.0|      9|               12|\n"
            + "|  5|                               36.0|      3|                8|\n"
            + "|  6|                               48.0|      6|                6|\n"
            + "+---+-----------------------------------+-------+-----------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testExprFilter() {
    String insert =
        "INSERT INTO test(key, s1, s2, s3) VALUES (1, 2, 3, 2), (2, 3, 1, 3), (3, 4, 3, 1), (4, 9, 7, 5), (5, 3, 6, 2), (6, 6, 4, 2);";
    tool.execute(insert);

    String query = "SELECT * FROM test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+-------+-------+-------+\n"
            + "|key|test.s1|test.s2|test.s3|\n"
            + "+---+-------+-------+-------+\n"
            + "|  1|      2|      3|      2|\n"
            + "|  2|      3|      1|      3|\n"
            + "|  3|      4|      3|      1|\n"
            + "|  4|      9|      7|      5|\n"
            + "|  5|      3|      6|      2|\n"
            + "|  6|      6|      4|      2|\n"
            + "+---+-------+-------+-------+\n"
            + "Total line number = 6\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT multiply(s1, s2) FROM test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+--------------------------+\n"
            + "|key|multiply(test.s1, test.s2)|\n"
            + "+---+--------------------------+\n"
            + "|  1|                       6.0|\n"
            + "|  2|                       3.0|\n"
            + "|  3|                      12.0|\n"
            + "|  4|                      63.0|\n"
            + "|  5|                      18.0|\n"
            + "|  6|                      24.0|\n"
            + "+---+--------------------------+\n"
            + "Total line number = 6\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT * FROM test WHERE multiply(s1, s2) > 15;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+-------+\n"
            + "|key|test.s1|test.s2|test.s3|\n"
            + "+---+-------+-------+-------+\n"
            + "|  4|      9|      7|      5|\n"
            + "|  5|      3|      6|      2|\n"
            + "|  6|      6|      4|      2|\n"
            + "+---+-------+-------+-------+\n"
            + "Total line number = 3\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT * FROM test WHERE multiply(s1, s2) + 10 > 15;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+-------+\n"
            + "|key|test.s1|test.s2|test.s3|\n"
            + "+---+-------+-------+-------+\n"
            + "|  1|      2|      3|      2|\n"
            + "|  3|      4|      3|      1|\n"
            + "|  4|      9|      7|      5|\n"
            + "|  5|      3|      6|      2|\n"
            + "|  6|      6|      4|      2|\n"
            + "+---+-------+-------+-------+\n"
            + "Total line number = 5\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT * FROM test WHERE multiply(s1, s2) + 10 > s3 + 15;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+-------+\n"
            + "|key|test.s1|test.s2|test.s3|\n"
            + "+---+-------+-------+-------+\n"
            + "|  3|      4|      3|      1|\n"
            + "|  4|      9|      7|      5|\n"
            + "|  5|      3|      6|      2|\n"
            + "|  6|      6|      4|      2|\n"
            + "+---+-------+-------+-------+\n"
            + "Total line number = 4\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT * FROM test WHERE multiply(s1, s2) + 9 > cos(s3) + 15;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+-------+\n"
            + "|key|test.s1|test.s2|test.s3|\n"
            + "+---+-------+-------+-------+\n"
            + "|  1|      2|      3|      2|\n"
            + "|  3|      4|      3|      1|\n"
            + "|  4|      9|      7|      5|\n"
            + "|  5|      3|      6|      2|\n"
            + "|  6|      6|      4|      2|\n"
            + "+---+-------+-------+-------+\n"
            + "Total line number = 5\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT * FROM test WHERE pow(s1, 0.5) - 1 > cos(s2) + cos(s3);";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+-------+\n"
            + "|key|test.s1|test.s2|test.s3|\n"
            + "+---+-------+-------+-------+\n"
            + "|  1|      2|      3|      2|\n"
            + "|  2|      3|      1|      3|\n"
            + "|  3|      4|      3|      1|\n"
            + "|  4|      9|      7|      5|\n"
            + "|  5|      3|      6|      2|\n"
            + "|  6|      6|      4|      2|\n"
            + "+---+-------+-------+-------+\n"
            + "Total line number = 6\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT * FROM test WHERE pow(s1 + s2, 2) - 5 > 30;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+-------+\n"
            + "|key|test.s1|test.s2|test.s3|\n"
            + "+---+-------+-------+-------+\n"
            + "|  3|      4|      3|      1|\n"
            + "|  4|      9|      7|      5|\n"
            + "|  5|      3|      6|      2|\n"
            + "|  6|      6|      4|      2|\n"
            + "+---+-------+-------+-------+\n"
            + "Total line number = 4\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT * FROM test WHERE multiply(s1, s2 + s3) > 20;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+-------+\n"
            + "|key|test.s1|test.s2|test.s3|\n"
            + "+---+-------+-------+-------+\n"
            + "|  4|      9|      7|      5|\n"
            + "|  5|      3|      6|      2|\n"
            + "|  6|      6|      4|      2|\n"
            + "+---+-------+-------+-------+\n"
            + "Total line number = 3\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testSelectFromUDF() {
    String insert =
        "INSERT INTO test(key, a, b) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    tool.execute(insert);

    List<Double> cosTestAExpectedValues =
        Arrays.asList(
            -0.4161468365471424,
            -0.9899924966004454,
            -0.6536436208636119,
            -0.9111302618846769,
            -0.9899924966004454,
            0.960170286650366);
    List<Double> cosTestBExpectedValues =
        Arrays.asList(
            -0.9899924966004454,
            0.5403023058681398,
            -0.9899924966004454,
            0.7539022543433046,
            0.9601702866503661,
            -0.6536436208636119);

    String query = "SELECT `cos(test.a)` FROM(SELECT cos(*) FROM test);";
    SessionExecuteSqlResult ret = tool.execute(query);

    compareResult(1, ret.getPaths().size());
    compareResult("cos(test.a)", ret.getPaths().get(0));

    for (int i = 0; i < ret.getValues().size(); i++) {
      compareResult(1, ret.getValues().get(i).size());
      double expected = cosTestAExpectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      compareResult(expected, actual, delta);
    }

    query = "SELECT `cos(test.b)` AS cos_b FROM(SELECT cos(*) FROM test);";
    ret = tool.execute(query);

    compareResult(1, ret.getPaths().size());
    compareResult("cos_b", ret.getPaths().get(0));

    for (int i = 0; i < ret.getValues().size(); i++) {
      compareResult(1, ret.getValues().get(i).size());
      double expected = cosTestBExpectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      compareResult(expected, actual, delta);
    }
  }

  @Test
  public void testTransposeRows() {
    String insert =
        "INSERT INTO test(key, a, b) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    tool.execute(insert);

    String query = "SELECT * FROM test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+------+------+\n"
            + "|key|test.a|test.b|\n"
            + "+---+------+------+\n"
            + "|  1|     2|     3|\n"
            + "|  2|     3|     1|\n"
            + "|  3|     4|     3|\n"
            + "|  4|     9|     7|\n"
            + "|  5|     3|     6|\n"
            + "|  6|     6|     4|\n"
            + "+---+------+------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT transpose(*) FROM (SELECT * FROM test);";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+------------+------------+------------+------------+------------+------------+\n"
            + "|transpose(0)|transpose(1)|transpose(2)|transpose(3)|transpose(4)|transpose(5)|\n"
            + "+------------+------------+------------+------------+------------+------------+\n"
            + "|           2|           3|           4|           9|           3|           6|\n"
            + "|           3|           1|           3|           7|           6|           4|\n"
            + "+------------+------------+------------+------------+------------+------------+\n"
            + "Total line number = 2\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query =
        "SELECT `transpose(0)`, `transpose(1)`, `transpose(2)` FROM (SELECT transpose(*) FROM (SELECT * FROM test));";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+------------+------------+------------+\n"
            + "|transpose(0)|transpose(1)|transpose(2)|\n"
            + "+------------+------------+------------+\n"
            + "|           2|           3|           4|\n"
            + "|           3|           1|           3|\n"
            + "+------------+------------+------------+\n"
            + "Total line number = 2\n";
    compareResult(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testColumnExpand() {
    String insert =
        "INSERT INTO test(key, a) VALUES (1, 2), (2, 3), (3, 4), (4, 9), (5, 3), (6, 6);";
    tool.execute(insert);

    String query = "SELECT a FROM test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+------+\n"
            + "|key|test.a|\n"
            + "+---+------+\n"
            + "|  1|     2|\n"
            + "|  2|     3|\n"
            + "|  3|     4|\n"
            + "|  4|     9|\n"
            + "|  5|     3|\n"
            + "|  6|     6|\n"
            + "+---+------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT columnExpand(*) FROM (SELECT a FROM test);";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+--------------------+------------------------+----------------------+\n"
            + "|key|columnExpand(test.a)|columnExpand(test.a+1.5)|columnExpand(test.a*2)|\n"
            + "+---+--------------------+------------------------+----------------------+\n"
            + "|  1|                   2|                     3.5|                     4|\n"
            + "|  2|                   3|                     4.5|                     6|\n"
            + "|  3|                   4|                     5.5|                     8|\n"
            + "|  4|                   9|                    10.5|                    18|\n"
            + "|  5|                   3|                     4.5|                     6|\n"
            + "|  6|                   6|                     7.5|                    12|\n"
            + "+---+--------------------+------------------------+----------------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query =
        "SELECT `columnExpand(test.a+1.5)` FROM (SELECT columnExpand(*) FROM (SELECT a FROM test)) WHERE `columnExpand(test.a+1.5)` < 5;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+------------------------+\n"
            + "|key|columnExpand(test.a+1.5)|\n"
            + "+---+------------------------+\n"
            + "|  1|                     3.5|\n"
            + "|  2|                     4.5|\n"
            + "|  5|                     4.5|\n"
            + "+---+------------------------+\n"
            + "Total line number = 3\n";
    compareResult(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUDFGroupByAndOrderByExpr() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 2, 3), (4, 3, 7), (5, 3, 6), (6, 0, 4);";
    tool.execute(insert);

    List<Double> cosTestS1AfterGroupByExpectedValues =
        Arrays.asList(-0.9899924966004454, -0.4161468365471424, 1.0);
    List<Long> sumTestS2AfterGroupByExpectedValues = Arrays.asList(14L, 6L, 4L);

    String query = "SELECT cos(s1), sum(s2) FROM test GROUP BY cos(s1) ORDER BY cos(s1);";
    SessionExecuteSqlResult ret = tool.execute(query);
    compareResult(2, ret.getPaths().size());
    compareResult("cos(test.s1)", ret.getPaths().get(0));
    compareResult("sum(test.s2)", ret.getPaths().get(1));
    for (int i = 0; i < ret.getValues().size(); i++) {
      compareResult(2, ret.getValues().get(i).size());
      double expectedCosS1 = cosTestS1AfterGroupByExpectedValues.get(i);
      double actualCosS1 = (double) ret.getValues().get(i).get(0);
      compareResult(expectedCosS1, actualCosS1, delta);
      long expectedSumS2 = sumTestS2AfterGroupByExpectedValues.get(i);
      long actualSumS2 = (long) ret.getValues().get(i).get(1);
      assertEquals(expectedSumS2, actualSumS2);
    }

    query = "SELECT cos(s1) AS a, sum(s2) FROM test GROUP BY a ORDER BY a;";
    ret = tool.execute(query);
    compareResult(2, ret.getPaths().size());
    compareResult("a", ret.getPaths().get(0));
    compareResult("sum(test.s2)", ret.getPaths().get(1));
    for (int i = 0; i < ret.getValues().size(); i++) {
      compareResult(2, ret.getValues().get(i).size());
      double expectedCosS1 = cosTestS1AfterGroupByExpectedValues.get(i);
      double actualCosS1 = (double) ret.getValues().get(i).get(0);
      compareResult(expectedCosS1, actualCosS1, delta);
      long expectedSumS2 = sumTestS2AfterGroupByExpectedValues.get(i);
      long actualSumS2 = (long) ret.getValues().get(i).get(1);
      assertEquals(expectedSumS2, actualSumS2);
    }

    query = "SELECT s1, s2 FROM test ORDER BY cos(s1);";
    ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+-------+-------+\n"
            + "|key|test.s1|test.s2|\n"
            + "+---+-------+-------+\n"
            + "|  2|      3|      1|\n"
            + "|  4|      3|      7|\n"
            + "|  5|      3|      6|\n"
            + "|  1|      2|      3|\n"
            + "|  3|      2|      3|\n"
            + "|  6|      0|      4|\n"
            + "+---+-------+-------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT s1, s2 FROM test ORDER BY pow(s2, 2);";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+-------+-------+\n"
            + "|key|test.s1|test.s2|\n"
            + "+---+-------+-------+\n"
            + "|  2|      3|      1|\n"
            + "|  1|      2|      3|\n"
            + "|  3|      2|      3|\n"
            + "|  6|      0|      4|\n"
            + "|  5|      3|      6|\n"
            + "|  4|      3|      7|\n"
            + "+---+-------+-------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUDFWithArgs() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    tool.execute(insert);

    String query = "SELECT pow(s1, 2) FROM test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+---------------+\n"
            + "|key|pow(test.s1, 2)|\n"
            + "+---+---------------+\n"
            + "|  1|            4.0|\n"
            + "|  2|            9.0|\n"
            + "|  3|           16.0|\n"
            + "|  4|           81.0|\n"
            + "|  5|            9.0|\n"
            + "|  6|           36.0|\n"
            + "+---+---------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT pow(s1, s2, 2) FROM test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+---------------+---------------+\n"
            + "|key|pow(test.s1, 2)|pow(test.s2, 2)|\n"
            + "+---+---------------+---------------+\n"
            + "|  1|            4.0|            9.0|\n"
            + "|  2|            9.0|            1.0|\n"
            + "|  3|           16.0|            9.0|\n"
            + "|  4|           81.0|           49.0|\n"
            + "|  5|            9.0|           36.0|\n"
            + "|  6|           36.0|           16.0|\n"
            + "+---+---------------+---------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT pow(*, 3) FROM test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+---------------+---------------+\n"
            + "|key|pow(test.s1, 3)|pow(test.s2, 3)|\n"
            + "+---+---------------+---------------+\n"
            + "|  1|            8.0|           27.0|\n"
            + "|  2|           27.0|            1.0|\n"
            + "|  3|           64.0|           27.0|\n"
            + "|  4|          729.0|          343.0|\n"
            + "|  5|           27.0|          216.0|\n"
            + "|  6|          216.0|           64.0|\n"
            + "+---+---------------+---------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUDFWithKvargs() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    tool.execute(insert);

    String query = "SELECT pow(s1, n=2) FROM test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+---------------+\n"
            + "|key|pow(test.s1, 2)|\n"
            + "+---+---------------+\n"
            + "|  1|            4.0|\n"
            + "|  2|            9.0|\n"
            + "|  3|           16.0|\n"
            + "|  4|           81.0|\n"
            + "|  5|            9.0|\n"
            + "|  6|           36.0|\n"
            + "+---+---------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT pow(s1, s2, n=2) FROM test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+---------------+---------------+\n"
            + "|key|pow(test.s1, 2)|pow(test.s2, 2)|\n"
            + "+---+---------------+---------------+\n"
            + "|  1|            4.0|            9.0|\n"
            + "|  2|            9.0|            1.0|\n"
            + "|  3|           16.0|            9.0|\n"
            + "|  4|           81.0|           49.0|\n"
            + "|  5|            9.0|           36.0|\n"
            + "|  6|           36.0|           16.0|\n"
            + "+---+---------------+---------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));

    query = "SELECT pow(*, n=3) FROM test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+---+---------------+---------------+\n"
            + "|key|pow(test.s1, 3)|pow(test.s2, 3)|\n"
            + "+---+---------------+---------------+\n"
            + "|  1|            8.0|           27.0|\n"
            + "|  2|           27.0|            1.0|\n"
            + "|  3|           64.0|           27.0|\n"
            + "|  4|          729.0|          343.0|\n"
            + "|  5|           27.0|          216.0|\n"
            + "|  6|          216.0|           64.0|\n"
            + "+---+---------------+---------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUDFWithArgsAndKvArgs() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    tool.execute(insert);

    String query = "SELECT pow(s1, 2), pow(s2, n=2) FROM test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+---------------+---------------+\n"
            + "|key|pow(test.s1, 2)|pow(test.s2, 2)|\n"
            + "+---+---------------+---------------+\n"
            + "|  1|            4.0|            9.0|\n"
            + "|  2|            9.0|            1.0|\n"
            + "|  3|           16.0|            9.0|\n"
            + "|  4|           81.0|           49.0|\n"
            + "|  5|            9.0|           36.0|\n"
            + "|  6|           36.0|           16.0|\n"
            + "+---+---------------+---------------+\n"
            + "Total line number = 6\n";
    compareResult(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testErrorClause() {
    String errClause = "select s1, s2, count(s3) from us.d1 group by reverse_rows(s1);";
    tool.executeAndCompareErrMsg(
        errClause, "GROUP BY column can not use SetToSet/SetToRow functions.");

    errClause = "select s1, s2, count(s3) from us.d1 group by s1, s2 order by transpose(s1);";
    tool.executeAndCompareErrMsg(
        errClause, "ORDER BY column can not use SetToSet/SetToRow functions.");
  }

  void compareResult(Object expected, Object actual) {
    if (!needCompareResult) {
      return;
    }
    assertEquals(expected, actual);
  }

  void compareResult(double expected, double actual, double delta) {
    if (!needCompareResult) {
      return;
    }
    assertEquals(expected, actual, delta);
  }

  void compareResult(long[] expected, long[] actual) {
    if (!needCompareResult) {
      return;
    }
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testUsingKeyInUDSF() {
    String insert = "INSERT INTO test(key, a) VALUES (1699950998000, 2), (1699951690000, 3);";
    tool.execute(insert);

    String query = "select reverse_rows(a) from test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+-------------+--------------------+\n"
            + "|          key|reverse_rows(test.a)|\n"
            + "+-------------+--------------------+\n"
            + "|1699951690000|                   3|\n"
            + "|1699950998000|                   2|\n"
            + "+-------------+--------------------+\n"
            + "Total line number = 2\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUsingKeyInUDAF() {
    String insert = "INSERT INTO test(key, a, b) VALUES (1,2,3), (2,3,4) (3,4,5);";
    tool.execute(insert);

    String query = "select udf_max_with_key(a) from test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+------------------------+\n"
            + "|key|udf_max_with_key(test.a)|\n"
            + "+---+------------------------+\n"
            + "|  3|                       4|\n"
            + "+---+------------------------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUsingKeyInUDTF() {
    String insert = "INSERT INTO test(key, a, b) VALUES (1,2,3), (2,3,4) (3,4,5);";
    tool.execute(insert);

    String query = "select key_add_one(a) from test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+-------------------+\n"
            + "|key|key_add_one(test.a)|\n"
            + "+---+-------------------+\n"
            + "|  2|                  2|\n"
            + "|  3|                  3|\n"
            + "|  4|                  4|\n"
            + "+---+-------------------+\n"
            + "Total line number = 3\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testRowTransform() {
    String insert = "INSERT INTO test(key, a, b) VALUES (1,2,3), (2,3,4) (3,4,5);";
    tool.execute(insert);

    String query = "select cos(a), pow(b, 2) from test;";
    SessionExecuteSqlResult ret = tool.execute(query);
    String expected =
        "ResultSets:\n"
            + "+---+-------------------+--------------+\n"
            + "|key|        cos(test.a)|pow(test.b, 2)|\n"
            + "+---+-------------------+--------------+\n"
            + "|  1|-0.4161468365471424|           9.0|\n"
            + "|  2|-0.9899924966004454|          16.0|\n"
            + "|  3|-0.6536436208636119|          25.0|\n"
            + "+---+-------------------+--------------+\n"
            + "Total line number = 3\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    if (isScaling) {
      return;
    }

    query = "explain select a, cos(a) from test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+-------------------+----------------+----------------------------------------------------------------------------------------+\n"
            + "|       Logical Tree|   Operator Type|                                                                           Operator Info|\n"
            + "+-------------------+----------------+----------------------------------------------------------------------------------------+\n"
            + "|RemoveNullColumn   |RemoveNullColumn|                                                                        RemoveNullColumn|\n"
            + "|  +--Reorder       |         Reorder|                                                               Order: test.a,cos(test.a)|\n"
            + "|    +--RowTransform|    RowTransform|FuncList(Name, FuncType): (arithmetic_expr, System), (cos, UDF), MappingType: RowMapping|\n"
            + "|      +--Project   |         Project|                                                                        Patterns: test.a|\n"
            + "|        +--Project |         Project|                                             Patterns: test.a, Target DU: unit0000000002|\n"
            + "+-------------------+----------------+----------------------------------------------------------------------------------------+\n"
            + "Total line number = 5\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "explain select cos(a), pow(b, 2) from test;";
    ret = tool.execute(query);
    expected =
        "ResultSets:\n"
            + "+-------------------+----------------+-------------------------------------------------------------------------+\n"
            + "|       Logical Tree|   Operator Type|                                                            Operator Info|\n"
            + "+-------------------+----------------+-------------------------------------------------------------------------+\n"
            + "|RemoveNullColumn   |RemoveNullColumn|                                                         RemoveNullColumn|\n"
            + "|  +--Reorder       |         Reorder|                                                                 Order: *|\n"
            + "|    +--RowTransform|    RowTransform|FuncList(Name, FuncType): (cos, UDF), (pow, UDF), MappingType: RowMapping|\n"
            + "|      +--Project   |         Project|                                                  Patterns: test.b,test.a|\n"
            + "|        +--Project |         Project|                       Patterns: test.a,test.b, Target DU: unit0000000002|\n"
            + "+-------------------+----------------+-------------------------------------------------------------------------+\n"
            + "Total line number = 5\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testImportModule() {
    String classPath = "my_module.sub_module.sub_class_a.SubClassA";
    String udfName = "module_udf_test";
    tool.executeReg(
        String.format(SINGLE_UDF_REGISTER_SQL, "udsf", udfName, classPath, MODULE_PATH));
    assertTrue(tool.isUDFRegistered(udfName));
    taskToBeRemoved.add(udfName);

    String insert = "insert into test(key, a) values (1,2);";
    tool.execute(insert);
    String query = "select " + udfName + "(a, 1) from test;";
    SessionExecuteSqlResult ret = tool.execute(query);

    String expected =
        "ResultSets:\n"
            + "+---------+\n"
            + "|col_inner|\n"
            + "+---------+\n"
            + "|        1|\n"
            + "+---------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  // register multiple UDFs in one statement, module/file both allowed.
  // use same type for all UDF in statement.
  @Test
  public void testMultiUDFRegOmit() {
    // ClassA & ClassB in same python file, & SubClassA in same module
    List<String> classPaths =
        new ArrayList<>(
            Arrays.asList(
                "my_module.my_class_a.ClassA",
                "my_module.my_class_a.ClassB",
                "my_module.sub_module.sub_class_a.SubClassA"));
    List<String> names = new ArrayList<>(Arrays.asList("udf_a", "udf_b", "udf_sub"));
    String registerSql = tool.concatMultiUDFReg("udsf", names, classPaths, MODULE_PATH);
    tool.executeReg(registerSql);
    assertTrue(tool.isUDFsRegistered(names));
    taskToBeRemoved.addAll(names);

    // test UDFs' usage
    String statement = "select udf_a(s1,1) from us.d1 where s1 < 10;";
    SessionExecuteSqlResult ret = tool.execute(statement);
    String expected =
        "ResultSets:\n"
            + "+-----------+\n"
            + "|col_outer_a|\n"
            + "+-----------+\n"
            + "|          1|\n"
            + "+-----------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    statement = "select udf_b(s1,1) from us.d1 where s1 < 10;";
    ret = tool.execute(statement);
    expected =
        "ResultSets:\n"
            + "+-----------+\n"
            + "|col_outer_b|\n"
            + "+-----------+\n"
            + "|          1|\n"
            + "+-----------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    // make sure "udf_b" is dropped and cannot be used
    tool.execute(String.format(DROP_SQL, "udf_b"));
    assertFalse(tool.isUDFRegistered("udf_b"));
    taskToBeRemoved.remove("udf_b");
    tool.executeFail(statement);

    // other udfs in the same module should work normally, use new udf to avoid cache.
    statement = "select udf_sub(s1,1) from us.d1 where s1 < 10;";
    ret = tool.execute(statement);
    expected =
        "ResultSets:\n"
            + "+---------+\n"
            + "|col_inner|\n"
            + "+---------+\n"
            + "|        1|\n"
            + "+---------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  // register multiple UDFs in one statement, module/file both allowed.
  // specify different type for each UDF in statement.
  @Test
  public void testMultiUDFRegSep() {
    List<String> types = new ArrayList<>(Arrays.asList("udtf", "udsf", "udaf"));
    // ClassA & ClassB in same python file, & SubClassA in same module
    List<String> classPaths =
        new ArrayList<>(
            Arrays.asList(
                "my_module.my_class_a.ClassA",
                "my_module.my_class_a.ClassB",
                "my_module.sub_module.sub_class_a.SubClassA"));
    List<String> names = new ArrayList<>(Arrays.asList("udf_a", "udf_b", "udf_sub"));
    String register = tool.concatMultiUDFReg(types, names, classPaths, MODULE_PATH);
    tool.executeReg(register);
    assertTrue(tool.isUDFsRegistered(names));
    taskToBeRemoved.addAll(names);

    // test UDFs' usage
    String statement = "select udf_a(s1,1) from us.d1 where s1 < 10;";
    SessionExecuteSqlResult ret = tool.execute(statement);
    String expected =
        "ResultSets:\n"
            + "+---+-----------+\n"
            + "|key|col_outer_a|\n"
            + "+---+-----------+\n"
            + "|  0|          1|\n"
            + "|  1|          1|\n"
            + "|  2|          1|\n"
            + "|  3|          1|\n"
            + "|  4|          1|\n"
            + "|  5|          1|\n"
            + "|  6|          1|\n"
            + "|  7|          1|\n"
            + "|  8|          1|\n"
            + "|  9|          1|\n"
            + "+---+-----------+\n"
            + "Total line number = 10\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    statement = "select udf_b(s1,1) from us.d1 where s1 < 10;";
    ret = tool.execute(statement);
    expected =
        "ResultSets:\n"
            + "+-----------+\n"
            + "|col_outer_b|\n"
            + "+-----------+\n"
            + "|          1|\n"
            + "+-----------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    // make sure "udf_b" is dropped and cannot be used
    tool.execute(String.format(DROP_SQL, "udf_b"));
    assertFalse(tool.isUDFRegistered("udf_b"));
    taskToBeRemoved.remove("udf_b");
    tool.executeFail(statement);

    // other udfs in the same module should work normally, use new udf to avoid cache.
    statement = "select udf_sub(s1,1) from us.d1 where s1 < 10;";
    ret = tool.execute(statement);
    expected =
        "ResultSets:\n"
            + "+---------+\n"
            + "|col_inner|\n"
            + "+---------+\n"
            + "|        1|\n"
            + "+---------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  // register multiple UDFs in one local python file
  @Test
  public void testMultiUDFReg() {
    List<String> types = new ArrayList<>(Arrays.asList("udtf", "udsf", "udaf"));
    // ClassA, ClassB & ClassC in same python file
    List<String> classPaths = new ArrayList<>(Arrays.asList("ClassA", "ClassB", "ClassC"));
    List<String> names = new ArrayList<>(Arrays.asList("udf_a", "udf_b", "udf_c"));
    String register = tool.concatMultiUDFReg(types, names, classPaths, MODULE_FILE_PATH);
    tool.executeReg(register);
    assertTrue(tool.isUDFsRegistered(names));
    taskToBeRemoved.addAll(names);

    // test UDFs' usage
    String statement = "select udf_a(s1,1) from us.d1 where s1 < 10;";
    SessionExecuteSqlResult ret = tool.execute(statement);
    String expected =
        "ResultSets:\n"
            + "+---+-----------+\n"
            + "|key|col_outer_a|\n"
            + "+---+-----------+\n"
            + "|  0|          1|\n"
            + "|  1|          1|\n"
            + "|  2|          1|\n"
            + "|  3|          1|\n"
            + "|  4|          1|\n"
            + "|  5|          1|\n"
            + "|  6|          1|\n"
            + "|  7|          1|\n"
            + "|  8|          1|\n"
            + "|  9|          1|\n"
            + "+---+-----------+\n"
            + "Total line number = 10\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    statement = "select udf_b(s1,1) from us.d1 where s1 < 10;";
    ret = tool.execute(statement);
    expected =
        "ResultSets:\n"
            + "+-----------+\n"
            + "|col_outer_b|\n"
            + "+-----------+\n"
            + "|          1|\n"
            + "+-----------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    // make sure "udf_b" is dropped and cannot be used
    tool.execute(String.format(DROP_SQL, "udf_b"));
    assertFalse(tool.isUDFRegistered("udf_b"));
    taskToBeRemoved.remove("udf_b");
    tool.executeFail(statement);

    // other udfs in the same file should work normally, use new udf to avoid cache.
    statement = "select udf_c(s1,1) from us.d1 where s1 < 10;";
    ret = tool.execute(statement);
    expected =
        "ResultSets:\n"
            + "+-----------+\n"
            + "|col_outer_c|\n"
            + "+-----------+\n"
            + "|          1|\n"
            + "+-----------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  // multiple UDFs registration should fail when:
  // 1. same class name
  // 2. same name
  // 3. counts of classes, types, names do not match (names and classes come in pairs so count of
  // types matters)
  @Test
  public void testMultiRegFail() {
    String register;
    List<String> types = new ArrayList<>(Arrays.asList("udtf", "udsf", "udaf"));
    List<String> classPaths =
        new ArrayList<>(
            Arrays.asList(
                "my_module.my_class_a.ClassA",
                "my_module.my_class_a.ClassB",
                "my_module.sub_module.sub_class_a.SubClassA"));
    List<String> names = new ArrayList<>(Arrays.asList("udf_a", "udf_b", "udf_sub"));

    // 2 types for 3 UDFs
    register =
        "create function "
            + types.get(0)
            + " \""
            + names.get(0)
            + "\" from \""
            + classPaths.get(0)
            + "\", "
            + types.get(1)
            + " \""
            + names.get(1)
            + "\" from \""
            + classPaths.get(1)
            + "\", "
            + "\""
            + names.get(2)
            + "\" from \""
            + classPaths.get(2)
            + "\" in \""
            + MODULE_PATH
            + "\";";
    tool.executeRegFail(register);
    assertTrue(tool.isUDFsUnregistered(names));

    // same class name
    List<String> classPathWrong =
        new ArrayList<>(
            Arrays.asList(
                "my_module.my_class_a.ClassA",
                "my_module.my_class_a.ClassB",
                "my_module.my_class_a.ClassB"));
    register = tool.concatMultiUDFReg(types, names, classPathWrong, MODULE_PATH);
    tool.executeRegFail(register);
    assertTrue(tool.isUDFsUnregistered(names));

    // same name
    List<String> nameWrong = new ArrayList<>(Arrays.asList("udf_a", "udf_b", "udf_b"));
    register = tool.concatMultiUDFReg(types, nameWrong, classPaths, MODULE_PATH);
    tool.executeRegFail(register);
    assertTrue(tool.isUDFsUnregistered(names));
  }

  @Test
  public void testModuleInstall() {
    String classPath = "my_module.dateutil_test.Test";
    String name = "dateutil_test";
    String type = "udsf";
    tool.executeReg(String.format(SINGLE_UDF_REGISTER_SQL, type, name, classPath, MODULE_PATH));
    assertTrue(tool.isUDFRegistered(name));
    taskToBeRemoved.add(name);

    // test UDFs' usage
    String statement = "select " + name + "(s1,1) from us.d1 where s1 < 10;";
    SessionExecuteSqlResult ret = tool.execute(statement);
    String expected =
        "ResultSets:\n"
            + "+---+----+------+-----+----+\n"
            + "|day|hour|minute|month|year|\n"
            + "+---+----+------+-----+----+\n"
            + "|  5|  14|    30|    4|2023|\n"
            + "+---+----+------+-----+----+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  // module with illegal requirements.txt cannot be registered.
  @Test
  public void testModuleInstallFail() {
    String newFileName = "requirements_backup.txt";
    String classPath = "my_module.dateutil_test.Test";
    String name = "dateutil_test";
    String type = "udsf";
    File reqFile = new File(String.join(File.separator, MODULE_PATH, "requirements.txt"));
    File renamedFile = new File(String.join(File.separator, MODULE_PATH, newFileName));
    String statement = String.format(SINGLE_UDF_REGISTER_SQL, type, name, classPath, MODULE_PATH);
    try {
      FileUtils.copyFileOrDir(reqFile, renamedFile);
    } catch (IOException e) {
      LOGGER.error("Can't rename file:{}.", reqFile, e);
      fail();
    }

    // append an illegal package(wrong name)
    try {
      FileUtils.appendFile(reqFile, "\nillegal-package");
      tool.executeRegFail(statement);
      assertFalse(tool.isUDFRegistered(name));
    } catch (IOException e) {
      LOGGER.error("Append content to file:{} failed.", reqFile, e);
      fail();
    } finally {
      try {
        FileUtils.deleteFileOrDir(reqFile);
        FileUtils.moveFile(renamedFile, reqFile);
      } catch (IOException ee) {
        LOGGER.error("Fail to recover requirement.txt .", ee);
      }
    }
  }

  @Test
  public void tensorUDFTest() {
    boolean torchSupported = System.getenv().getOrDefault("TORCH_SUPPORTED", "true").equals("true");
    Assume.assumeTrue(
        "tensorUDFTest is skipped because pytorch is not supported(python>3.12).", torchSupported);
    String name = "tensorTest";
    String filePath =
        String.join(
            File.separator,
            System.getProperty("user.dir"),
            "src",
            "test",
            "resources",
            "udf",
            "tensor_test.py");
    String statement = String.format(SINGLE_UDF_REGISTER_SQL, "udsf", name, "TensorTest", filePath);
    tool.executeReg(statement);
    assertTrue(tool.isUDFRegistered(name));
    taskToBeRemoved.add(name);

    statement = "select " + name + "(s1) from us.d1 where s1 < 10;";
    SessionExecuteSqlResult ret = tool.execute(statement);
    String expected =
        "ResultSets:\n"
            + "+--------------------+\n"
            + "|tensorTest(us.d1.s1)|\n"
            + "+--------------------+\n"
            + "|                 0.0|\n"
            + "+--------------------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    // test twice to ensure
    statement = "select " + name + "(s1) from us.d1 where s1 < 10;";
    ret = tool.execute(statement);
    expected =
        "ResultSets:\n"
            + "+--------------------+\n"
            + "|tensorTest(us.d1.s1)|\n"
            + "+--------------------+\n"
            + "|                 0.0|\n"
            + "+--------------------+\n"
            + "Total line number = 1\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUDFColumnPruning() {
    String statement = "SELECT cos(s1), cos(s2) FROM us.d1 LIMIT 5;";
    String expected =
        "ResultSets:\n"
            + "+---+-------------------+-------------------+\n"
            + "|key|      cos(us.d1.s1)|      cos(us.d1.s2)|\n"
            + "+---+-------------------+-------------------+\n"
            + "|  0|                1.0| 0.5403023058681398|\n"
            + "|  1| 0.5403023058681398|-0.4161468365471424|\n"
            + "|  2|-0.4161468365471424|-0.9899924966004454|\n"
            + "|  3|-0.9899924966004454|-0.6536436208636119|\n"
            + "|  4|-0.6536436208636119|0.28366218546322625|\n"
            + "+---+-------------------+-------------------+\n"
            + "Total line number = 5\n";

    assertEquals(expected, tool.execute(statement).getResultInString(false, ""));
  }
}
