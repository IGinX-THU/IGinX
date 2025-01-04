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
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

  public static List<List<Long>> readTimeCostsFromFile(String filename) {
    List<String> lines = getLinesFromFile(filename);
    assert lines.size() == 22;

    // TPC-H一共有22条查询
    List<List<Long>> timeCosts = new ArrayList<>(22);
    for (int i = 0; i < 22; i++) {
      List<Long> tmp = new ArrayList<>();
      String[] lineArray = lines.get(i).split(",");
      for (String str : lineArray) {
        if (!str.isEmpty()) {
          tmp.add(Long.parseLong(str));
        }
      }
      timeCosts.add(tmp);
    }
    return timeCosts;
  }

  public static void clearAndRewriteTimeCostsToFile(List<List<Long>> timeCosts, String filename) {
    try (FileWriter fileWriter = new FileWriter(filename);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      // 清空文件内容
      fileWriter.write("");
      fileWriter.flush();
      // 重新写入内容
      for (List<Long> timeCost : timeCosts) {
        bufferedWriter.write(
            timeCost.stream().map(String::valueOf).collect(Collectors.joining(",")));
        bufferedWriter.newLine();
      }
    } catch (IOException e) {
      LOGGER.error("Write to file {} fail. Caused by:", filename, e);
      Assert.fail();
    }
  }

  public static List<String> getLinesFromFile(String fileName) {
    Path filePath = Paths.get(fileName);
    List<String> lines = null;
    try {
      lines = Files.readAllLines(filePath);
    } catch (IOException e) {
      LOGGER.error("Read file {} fail. Caused by:", filePath, e);
      Assert.fail();
    }
    return lines;
  }

  public static long executeTPCHQuery(Session session, int queryId, boolean needValidate) {
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

  private static void validate(SessionExecuteSqlResult result, int queryId) {
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
