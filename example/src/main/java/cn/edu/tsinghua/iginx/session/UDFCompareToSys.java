/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;

public class UDFCompareToSys {

  private static Session session;

  private static final long START_TIMESTAMP = 0L;

  private static final long END_TIMESTAMP = 150000L;

  private static final int RETRY_TIMES = 5;

  private static final List<String> FUNC_LIST = Arrays.asList("min", "max", "sum", "avg", "count");

  public static void main(String[] args) throws SessionException {
    setUp();
    insertData();

    wholeRangeAggregationQuery();

    partialRangeAggregationQuery();

    multiPathWholeRangeAggregationQuery();

    multiPathPartialRangeAggregationQuery();

    wholeRangeGroupByQuery();

    partialRangeGroupByQuery();

    multiPathWholeRangeGroupByQuery();

    multiPathPartialRangeGroupByQuery();

    clearData();
    tearDown();
  }

  private static void wholeRangeAggregationQuery() throws SessionException {
    String SQLFormatter = "SELECT %s(s1) FROM test.compare;";
    for (String func : FUNC_LIST) {
      String sysSql = String.format(SQLFormatter, func);
      String udfSql = String.format(SQLFormatter, "udf_" + func);

      runAndCompare(sysSql, udfSql);
    }
  }

  private static void partialRangeAggregationQuery() throws SessionException {
    String SQLFormatter = "SELECT %s(s1) FROM test.compare WHERE key < 50;";
    for (String func : FUNC_LIST) {
      String sysSql = String.format(SQLFormatter, func);
      String udfSql = String.format(SQLFormatter, "udf_" + func);

      runAndCompare(sysSql, udfSql);
    }
  }

  private static void multiPathWholeRangeAggregationQuery() throws SessionException {
    String SQLFormatter = "SELECT %s(s1), %s(s2) FROM test.compare;";
    for (String func : FUNC_LIST) {
      String sysSql = String.format(SQLFormatter, func, func);
      String udfSql = String.format(SQLFormatter, "udf_" + func, "udf_" + func);

      runAndCompare(sysSql, udfSql);
    }
  }

  private static void multiPathPartialRangeAggregationQuery() throws SessionException {
    String SQLFormatter = "SELECT %s(s1), %s(s2) FROM test.compare WHERE key < 50;";
    for (String func : FUNC_LIST) {
      String sysSql = String.format(SQLFormatter, func, func);
      String udfSql = String.format(SQLFormatter, "udf_" + func, "udf_" + func);

      runAndCompare(sysSql, udfSql);
    }
  }

  private static void wholeRangeGroupByQuery() throws SessionException {
    String SQLFormatter = "SELECT %s(s1) FROM test.compare GROUP [%s, %s] BY 50s;";
    for (String func : FUNC_LIST) {
      String sysSql = String.format(SQLFormatter, func, START_TIMESTAMP, END_TIMESTAMP);
      String udfSql = String.format(SQLFormatter, "udf_" + func, START_TIMESTAMP, END_TIMESTAMP);

      runAndCompare(sysSql, udfSql);
    }
  }

  private static void partialRangeGroupByQuery() throws SessionException {
    String SQLFormatter = "SELECT %s(s1) FROM test.compare GROUP [0, 200] BY 50s;";
    for (String func : FUNC_LIST) {
      String sysSql = String.format(SQLFormatter, func);
      String udfSql = String.format(SQLFormatter, "udf_" + func);

      runAndCompare(sysSql, udfSql);
    }
  }

  private static void multiPathWholeRangeGroupByQuery() throws SessionException {
    String SQLFormatter = "SELECT %s(s1), %s(s2) FROM test.compare GROUP [%s, %s] BY 50s;";
    for (String func : FUNC_LIST) {
      String sysSql = String.format(SQLFormatter, func, func, START_TIMESTAMP, END_TIMESTAMP);
      String udfSql =
          String.format(SQLFormatter, "udf_" + func, "udf_" + func, START_TIMESTAMP, END_TIMESTAMP);

      runAndCompare(sysSql, udfSql);
    }
  }

  private static void multiPathPartialRangeGroupByQuery() throws SessionException {
    String SQLFormatter = "SELECT %s(s1), %s(s2) FROM test.compare GROUP [0, 200] BY 50s;";
    for (String func : FUNC_LIST) {
      String sysSql = String.format(SQLFormatter, func, func);
      String udfSql = String.format(SQLFormatter, "udf_" + func, "udf_" + func);

      runAndCompare(sysSql, udfSql);
    }
  }

  private static void runAndCompare(String sysSql, String udfSql) throws SessionException {
    double sysCostTime = runAndRecordTime(sysSql, RETRY_TIMES);
    double udfCostTime = runAndRecordTime(udfSql, RETRY_TIMES);

    System.out.println(sysSql);
    System.out.println(
        String.format(
            "sys cost: %s ms, udf cost: %s ms, rate: %.4f",
            sysCostTime, udfCostTime, sysCostTime / udfCostTime));
  }

  private static double runAndRecordTime(String sql, int retryTimes) throws SessionException {
    long startTime, endTime;

    double totalTime = 0.0;
    for (int i = 0; i < retryTimes; i++) {
      startTime = System.currentTimeMillis();
      session.executeSql(sql);
      endTime = System.currentTimeMillis();
      totalTime += endTime - startTime;
    }

    return totalTime / retryTimes;
  }

  public static void setUp() {
    session = new Session("127.0.0.1", 6888, "root", "root");
    try {
      session.openSession();
    } catch (SessionException e) {
      System.out.println(e.getMessage());
    }
  }

  public static void tearDown() {
    try {
      session.closeSession();
    } catch (SessionException e) {
      System.out.println(e.getMessage());
    }
  }

  public static void insertData() throws SessionException {
    String insertStrPrefix = "INSERT INTO test.compare (key, s1, s2, s3, s4) values ";

    StringBuilder builder = new StringBuilder(insertStrPrefix);

    int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
    for (int i = 0; i < size; i++) {
      builder.append(", ");
      builder.append("(");
      builder.append(START_TIMESTAMP + i).append(", ");
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
      System.out.println("Insert date execute fail. Caused by: " + res.getParseErrorMsg());
    }
  }

  public static void clearData() throws SessionException {
    String clearData = "CLEAR DATA;";

    SessionExecuteSqlResult res = session.executeSql(clearData);
    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      System.out.println("Clear date execute fail. Caused by: " + res.getParseErrorMsg());
    }
  }
}
