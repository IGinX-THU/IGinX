/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.integration.func.udf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskInfo;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFIT {

  private static final double delta = 0.01d;

  private static final Logger logger = LoggerFactory.getLogger(UDFIT.class);

  private static Session session;

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    session.closeSession();
  }

  @Before
  public void insertData() throws ExecutionException, SessionException {
    String insertStrPrefix = "INSERT INTO us.d1 (key, s1, s2, s3, s4) values ";

    long startKey = 0L;
    long endKey = 15000L;

    StringBuilder builder = new StringBuilder(insertStrPrefix);

    int size = (int) (endKey - startKey);
    for (int i = 0; i < size; i++) {
      builder.append(", ");
      builder.append("(");
      builder.append(startKey + i).append(", ");
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

    String insertStatement = builder.toString();

    SessionExecuteSqlResult res = session.executeSql(insertStatement);
    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      logger.error("Insert date execute fail. Caused by: {}.", res.getParseErrorMsg());
      fail();
    }
  }

  @After
  public void clearData() {
    Controller.clearData(session);
  }

  private SessionExecuteSqlResult execute(String statement) {
    logger.info("Execute Statement: \"{}\"", statement);

    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(statement);
    } catch (SessionException | ExecutionException e) {
      logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      logger.error(
          "Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
      fail();
    }

    return res;
  }

  @Test
  public void baseTests() {
    String showRegisterUDF = "SHOW REGISTER PYTHON TASK;";
    String udtfSQLFormat = "SELECT %s(s1) FROM us.d1 WHERE key < 200;";
    String udafSQLFormat = "SELECT %s(s1) FROM us.d1 OVER (RANGE 50 IN [0, 200));";
    String udsfSQLFormat = "SELECT %s(s1) FROM us.d1 WHERE key < 50;";

    SessionExecuteSqlResult ret = execute(showRegisterUDF);

    List<RegisterTaskInfo> registerUDFs = ret.getRegisterTaskInfos();
    for (RegisterTaskInfo info : registerUDFs) {
      // execute udf
      if (info.getType().equals(UDFType.UDTF)) {
        execute(String.format(udtfSQLFormat, info.getName()));
      } else if (info.getType().equals(UDFType.UDAF)) {
        execute(String.format(udafSQLFormat, info.getName()));
      } else if (info.getType().equals(UDFType.UDSF)) {
        execute(String.format(udsfSQLFormat, info.getName()));
      }
    }
  }

  @Test
  public void testCOS() {
    String statement = "SELECT cos(s1) FROM us.d1 WHERE s1 < 10;";

    SessionExecuteSqlResult ret = execute(statement);
    assertEquals(Collections.singletonList("cos(us.d1.s1)"), ret.getPaths());
    assertArrayEquals(new long[] {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L}, ret.getKeys());

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
      assertEquals(1, ret.getValues().get(i).size());
      double expected = expectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      assertEquals(expected, actual, delta);
    }
  }

  @Test
  public void testConcurrentCos() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    execute(insert);

    String query =
        "SELECT * FROM (SELECT cos(s1) AS cos_s1 FROM test) AS t1, (SELECT cos(s2) AS cos_s2 FROM test) AS t2 LIMIT 10;";
    SessionExecuteSqlResult ret = execute(query);
    assertEquals(4, ret.getPaths().size());

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
      assertEquals(4, ret.getValues().get(i).size());
      double expected = cosS1ExpectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      assertEquals(expected, actual, delta);

      expected = cosS2ExpectedValues.get(i);
      actual = (double) ret.getValues().get(i).get(2);
      assertEquals(expected, actual, delta);
    }
  }

  @Test
  public void testMultiColumns() {
    String insert =
        "INSERT INTO test(key, s1, s2, s3) VALUES (1, 2, 3, 2), (2, 3, 1, 3), (3, 4, 3, 1), (4, 9, 7, 5), (5, 3, 6, 2), (6, 6, 4, 2);";
    execute(insert);

    String query = "SELECT multiply(s1, s2) FROM test;";
    SessionExecuteSqlResult ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT multiply(s1, s2, s3) FROM test;";
    ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT multiply(s1, s2, s3), s1, s2 + s3 FROM test;";
    ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testExprFilter() {
    String insert =
        "INSERT INTO test(key, s1, s2, s3) VALUES (1, 2, 3, 2), (2, 3, 1, 3), (3, 4, 3, 1), (4, 9, 7, 5), (5, 3, 6, 2), (6, 6, 4, 2);";
    execute(insert);

    String query = "SELECT * FROM test;";
    SessionExecuteSqlResult ret = execute(query);
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
    ret = execute(query);
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
    ret = execute(query);
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
    ret = execute(query);
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
    ret = execute(query);
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
    ret = execute(query);
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
    ret = execute(query);
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
  }

  @Test
  public void testSelectFromUDF() {
    String insert =
        "INSERT INTO test(key, a, b) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    execute(insert);

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
    SessionExecuteSqlResult ret = execute(query);

    assertEquals(1, ret.getPaths().size());
    assertEquals("cos(test.a)", ret.getPaths().get(0));

    for (int i = 0; i < ret.getValues().size(); i++) {
      assertEquals(1, ret.getValues().get(i).size());
      double expected = cosTestAExpectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      assertEquals(expected, actual, delta);
    }

    query = "SELECT `cos(test.b)` AS cos_b FROM(SELECT cos(*) FROM test);";
    ret = execute(query);

    assertEquals(1, ret.getPaths().size());
    assertEquals("cos_b", ret.getPaths().get(0));

    for (int i = 0; i < ret.getValues().size(); i++) {
      assertEquals(1, ret.getValues().get(i).size());
      double expected = cosTestBExpectedValues.get(i);
      double actual = (double) ret.getValues().get(i).get(0);
      assertEquals(expected, actual, delta);
    }
  }

  @Test
  public void testTransposeRows() {
    String insert =
        "INSERT INTO test(key, a, b) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    execute(insert);

    String query = "SELECT * FROM test;";
    SessionExecuteSqlResult ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT transpose(*) FROM (SELECT * FROM test);";
    ret = execute(query);
    expected =
        "ResultSets:\n"
            + "+------------+------------+------------+------------+------------+------------+\n"
            + "|transpose(0)|transpose(1)|transpose(2)|transpose(3)|transpose(4)|transpose(5)|\n"
            + "+------------+------------+------------+------------+------------+------------+\n"
            + "|           2|           3|           4|           9|           3|           6|\n"
            + "|           3|           1|           3|           7|           6|           4|\n"
            + "+------------+------------+------------+------------+------------+------------+\n"
            + "Total line number = 2\n";
    assertEquals(expected, ret.getResultInString(false, ""));

    query =
        "SELECT `transpose(0)`, `transpose(1)`, `transpose(2)` FROM (SELECT transpose(*) FROM (SELECT * FROM test));";
    ret = execute(query);
    expected =
        "ResultSets:\n"
            + "+------------+------------+------------+\n"
            + "|transpose(0)|transpose(1)|transpose(2)|\n"
            + "+------------+------------+------------+\n"
            + "|           2|           3|           4|\n"
            + "|           3|           1|           3|\n"
            + "+------------+------------+------------+\n"
            + "Total line number = 2\n";
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testColumnExpand() {
    String insert =
        "INSERT INTO test(key, a) VALUES (1, 2), (2, 3), (3, 4), (4, 9), (5, 3), (6, 6);";
    execute(insert);

    String query = "SELECT a FROM test;";
    SessionExecuteSqlResult ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT columnExpand(*) FROM (SELECT a FROM test);";
    ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query =
        "SELECT `columnExpand(test.a+1.5)` FROM (SELECT columnExpand(*) FROM (SELECT a FROM test)) WHERE `columnExpand(test.a+1.5)` < 5;";
    ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUDFWithArgs() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    execute(insert);

    String query = "SELECT pow(s1, 2) FROM test;";
    SessionExecuteSqlResult ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT pow(s1, s2, 2) FROM test;";
    ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT pow(*, 3) FROM test;";
    ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUDFWithKvargs() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    execute(insert);

    String query = "SELECT pow(s1, n=2) FROM test;";
    SessionExecuteSqlResult ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT pow(s1, s2, n=2) FROM test;";
    ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));

    query = "SELECT pow(*, n=3) FROM test;";
    ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUDFWithArgsAndKvArgs() {
    String insert =
        "INSERT INTO test(key, s1, s2) VALUES (1, 2, 3), (2, 3, 1), (3, 4, 3), (4, 9, 7), (5, 3, 6), (6, 6, 4);";
    execute(insert);

    String query = "SELECT pow(s1, 2), pow(s2, n=2) FROM test;";
    SessionExecuteSqlResult ret = execute(query);
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
    assertEquals(expected, ret.getResultInString(false, ""));
  }

  @Test
  public void testUsingKeyInUDSF() {
    String insert = "INSERT INTO test(key, a) VALUES (1699950998000, 2), (1699951690000, 3);";
    execute(insert);

    String query = "select reverse_rows(a) from test;";
    SessionExecuteSqlResult ret = execute(query);
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
    execute(insert);

    String query = "select udf_max_with_key(a) from test;";
    SessionExecuteSqlResult ret = execute(query);
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
    execute(insert);

    String query = "select key_add_one(a) from test;";
    SessionExecuteSqlResult ret = execute(query);
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
}
