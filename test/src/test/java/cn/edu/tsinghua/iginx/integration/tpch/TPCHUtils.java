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
package cn.edu.tsinghua.iginx.integration.tpch;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPCHUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TPCHUtils.class);

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
        tmp.add(Long.parseLong(str));
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
        if (result.getPaths().get(j).contains("address")
            || result.getPaths().get(j).contains("comment")
            || result.getPaths().get(j).contains("orderdate")) {
          // TODO change unix time to date
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
          String resultString = new String((byte[]) values.get(i).get(j), StandardCharsets.UTF_8);
          String answerString = answers.get(i).get(j);
          if (!resultString.equals(answerString)) {
            System.out.println("Result string: " + resultString);
            System.out.println("Answer string: " + answerString);
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
