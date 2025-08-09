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
package cn.edu.tsinghua.iginx.integration.tpch;

import static cn.edu.tsinghua.iginx.integration.tpch.TPCHUtils.FieldType.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.io.MoreFiles;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPCHUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TPCHUtils.class);

  private static final String DATA_DIR =
      System.getProperty("user.dir") + "/../tpc/TPC-H V3.0.1/data";
  static final String FAILED_QUERY_ID_PATH =
      "src/test/resources/tpch/runtimeInfo/failedQueryIds.txt";

  static final String ITERATION_TIMES_PATH =
      "src/test/resources/tpch/runtimeInfo/iterationTimes.txt";

  static final String OLD_TIME_COSTS_PATH = "src/test/resources/tpch/runtimeInfo/oldTimeCosts.txt";

  static final String NEW_TIME_COSTS_PATH = "src/test/resources/tpch/runtimeInfo/newTimeCosts.txt";

  static final String STATUS_PATH = "src/test/resources/tpch/runtimeInfo/status.txt";

  enum FieldType {
    NUM,
    STR,
    DATE
  }

  public static void insert(Session session) throws IOException, SessionException, ParseException {
    insertTable(
        session,
        "region",
        Arrays.asList("r_regionkey", "r_name", "r_comment"),
        Arrays.asList(NUM, STR, STR));

    insertTable(
        session,
        "nation",
        Arrays.asList("n_nationkey", "n_name", "n_regionkey", "n_comment"),
        Arrays.asList(NUM, STR, NUM, STR));

    insertTable(
        session,
        "supplier",
        Arrays.asList(
            "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"),
        Arrays.asList(NUM, STR, STR, NUM, STR, NUM, STR));

    insertTable(
        session,
        "part",
        Arrays.asList(
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment"),
        Arrays.asList(NUM, STR, STR, STR, STR, NUM, STR, NUM, STR));

    insertTable(
        session,
        "partsupp",
        Arrays.asList("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"),
        Arrays.asList(NUM, NUM, NUM, NUM, STR));

    insertTable(
        session,
        "customer",
        Arrays.asList(
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment"),
        Arrays.asList(NUM, STR, STR, NUM, STR, NUM, STR, STR));

    insertTable(
        session,
        "orders",
        Arrays.asList(
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment"),
        Arrays.asList(NUM, NUM, STR, NUM, DATE, STR, STR, NUM, STR));

    insertTable(
        session,
        "lineitem",
        Arrays.asList(
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment"),
        Arrays.asList(
            NUM, NUM, NUM, NUM, NUM, NUM, NUM, NUM, STR, STR, DATE, DATE, DATE, STR, STR, STR));
  }

  private static void insertTable(
      Session session, String table, List<String> fields, List<FieldType> types)
      throws IOException, SessionException, ParseException {
    LOGGER.info("Inserting TPC-H table [{}].", table);

    StringBuilder builder = new StringBuilder("INSERT INTO ");
    builder.append(table);
    builder.append("(key, ");
    for (String field : fields) {
      builder.append(field);
      builder.append(", ");
    }
    builder.setLength(builder.length() - 2);
    builder.append(") VALUES ");
    String insertPrefix = builder.toString();

    long count = 0;
    try (BufferedReader br =
        new BufferedReader(new FileReader(String.format("%s/%s.tbl", DATA_DIR, table)))) {
      StringBuilder sb = new StringBuilder(insertPrefix);
      String line;
      while ((line = br.readLine()) != null) {
        String[] items = line.split("\\|");
        sb.append("(");
        sb.append(count); // 插入自增key列
        count++;
        sb.append(", ");
        assert fields.size() == items.length;
        for (int i = 0; i < items.length; i++) {
          switch (types.get(i)) {
            case NUM:
              sb.append(items[i]);
              sb.append(", ");
              break;
            case STR: // 字符串类型在外面需要包一层引号
              sb.append("\"");
              sb.append(items[i]);
              sb.append("\", ");
              break;
            case DATE: // 日期类型需要转为时间戳
              SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
              dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
              long time = dateFormat.parse(items[i]).getTime();
              sb.append(time);
              sb.append(", ");
              break;
            default:
              break;
          }
        }
        sb.setLength(sb.length() - 2);
        sb.append("), ");

        // 每次最多插入10000条数据
        if (count % 10000 == 0) {
          sb.setLength(sb.length() - 2);
          sb.append(";");
          session.executeSql(sb.toString());
          sb = new StringBuilder(insertPrefix);
        }
      }
      // 插入剩余数据
      if (sb.length() != insertPrefix.length()) {
        sb.setLength(sb.length() - 2);
        sb.append(";");
        session.executeSql(sb.toString());
      }
    }

    LOGGER.info("Insert {} records into table [{}].", count, table);
  }

  public static void showRules(Session session) {
    String SHOW_RULES_SQL = "show rules;";
    SessionExecuteSqlResult result = null;
    try {
      result = session.executeSql(SHOW_RULES_SQL);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", SHOW_RULES_SQL, e);
    }
    String s = "";
    if (result != null) {
      s = result.getResultInString(false, "");
    }
    LOGGER.info("Show rules results:\n{}", s);
  }

  public static String readSqlFileAsString(String filePath) throws IOException {
    StringBuilder contentBuilder = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = br.readLine()) != null) {
        contentBuilder.append(line).append("\n");
      }
    }
    return contentBuilder.toString();
  }

  public static ArrayListMultimap<String, Long> readTimeCostsFromFile(String filename)
      throws IOException {
    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(Paths.get(filename))) {
      properties.load(in);
    } catch (NoSuchFileException ignored) {
      return ArrayListMultimap.create();
    }
    ArrayListMultimap<String, Long> timeCosts = ArrayListMultimap.create();
    for (String key : properties.stringPropertyNames()) {
      String[] values = properties.getProperty(key).split(",");
      for (String value : values) {
        timeCosts.put(key, Long.parseLong(value));
      }
    }
    return timeCosts;
  }

  public static void clearAndRewriteTimeCostsToFile(
      ArrayListMultimap<String, Long> timeCosts, String filename) throws IOException {
    Properties properties = new Properties();
    for (Map.Entry<String, Collection<Long>> entry : timeCosts.asMap().entrySet()) {
      String key = entry.getKey();
      Collection<Long> values = entry.getValue();
      String value = values.stream().map(Object::toString).collect(Collectors.joining(","));
      properties.setProperty(key, value);
    }
    Path path = Paths.get(filename);
    MoreFiles.createParentDirectories(path);
    try (OutputStream out = Files.newOutputStream(path)) {
      properties.store(out, null);
    }
  }

  public static List<String> getFailedQueryIdsFromFile() throws IOException {
    Path path = Paths.get(FAILED_QUERY_ID_PATH);
    try {
      return Files.readAllLines(path);
    } catch (NoSuchFileException ignored) {
      ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
      return conf.getQueryIds();
    }
  }

  public static void clearAndRewriteFailedQueryIdsToFile(List<String> failedQueryIds)
      throws IOException {
    Path path = Paths.get(FAILED_QUERY_ID_PATH);
    MoreFiles.createParentDirectories(path);
    Files.write(path, failedQueryIds);
  }

  public static int getIterationTimesFromFile() throws IOException {
    try {
      List<String> lines = Files.readAllLines(Paths.get(ITERATION_TIMES_PATH));
      assert lines.size() == 1;
      return Integer.parseInt(lines.get(0));
    } catch (NoSuchFileException ignored) {
      return 1;
    }
  }

  public static void rewriteIterationTimes(long iterationTimes) throws IOException {
    Path path = Paths.get(ITERATION_TIMES_PATH);
    MoreFiles.createParentDirectories(path);
    Files.write(path, Collections.singleton(String.valueOf(iterationTimes)));
  }

  public static long executeTPCHQuery(Session session, String queryId, boolean needValidate) {
    String sqlString = null;
    try {
      sqlString =
          TPCHUtils.readSqlFileAsString("src/test/resources/tpch/queries/q" + queryId + ".sql");
    } catch (IOException e) {
      LOGGER.error("Fail to read sql file: q{}.sql. Caused by: ", queryId, e);
      Assert.fail();
    }
    String[] sqls = sqlString.split(";");
    String sql = sqls[sqls.length - 2] + ";";
    SessionExecuteSqlResult result = null;
    long startTime = System.currentTimeMillis();
    try {
      result = session.executeSql(sql);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", sql, e);
      Assert.fail();
    }
    long cost = System.currentTimeMillis() - startTime;
    if (needValidate) {
      validate(result, queryId);
    }
    return cost;
  }

  private static void validate(SessionExecuteSqlResult result, String queryId) {
    List<List<Object>> values = result.getValues();
    List<List<String>> answers = csvReader("src/test/resources/tpch/sf0.1/q" + queryId + ".csv");
    if (values.size() != answers.size()) {
      System.out.println("values.size() = " + values.size());
      System.out.println("answers.size() = " + answers.size());
      throw new RuntimeException("size not equal");
    }
    for (int i = 0; i < values.size(); i++) {
      for (int j = 0; j < values.get(i).size(); j++) {
        if (result.getPaths().get(j).contains("orderdate")) {
          long timestamp = (long) values.get(i).get(j);
          SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
          dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
          String date = dateFormat.format(new Date(timestamp));
          String answerDate = answers.get(i).get(j);
          if (!date.equals(answerDate)) {
            System.out.println("Result string: '" + date + "'");
            System.out.println("Answer string: '" + answerDate + "'");
          }
          assert date.equals(answerDate);
          continue;
        }
        // if only contains number and dot, then parse to double
        if (values.get(i).get(j).toString().matches("-?[0-9]+.*[0-9]*")) {
          double number = Double.parseDouble(values.get(i).get(j).toString());
          double answerNumber = Double.parseDouble(answers.get(i).get(j));
          if (answerNumber - number >= 1e-3 || number - answerNumber >= 1e-3) {
            System.out.println("Number: " + number);
            System.out.println("Answer number: " + answerNumber);
          }

          assert answerNumber - number < 1e-3 && number - answerNumber < 1e-3;
        } else {
          String resultString =
              new String((byte[]) values.get(i).get(j), StandardCharsets.UTF_8).trim();
          String answerString = answers.get(i).get(j).trim();
          if (!resultString.equals(answerString)) {
            System.out.println("Result string: '" + resultString + "'");
            System.out.println("Answer string: '" + answerString + "'");
          }
          assert resultString.equals(answerString);
        }
      }
    }
  }

  private static List<List<String>> csvReader(String filePath) {
    List<List<String>> data = new ArrayList<>();
    boolean skipHeader = true;
    try (Scanner scanner = new Scanner(Paths.get(filePath))) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        if (skipHeader) {
          skipHeader = false;
          continue;
        }
        List<String> row = Arrays.asList(line.split("\\|"));
        data.add(row);
      }
    } catch (IOException e) {
      LOGGER.error("Read file {} fail. Caused by:", filePath, e);
      Assert.fail();
    }
    return data;
  }
}
