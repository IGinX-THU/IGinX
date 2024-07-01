package cn.edu.tsinghua.iginx.integration.polybench;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.Test;

public class TPCHRunner {
  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";
  protected static MultiConnection conn;

  public static void TPCRunner(String[] args) {}

  public static void writeToFile(List<Double> avgTimeCosts, String fileName) throws IOException {
    Path filePath = Paths.get(fileName);
    List<String> lines = avgTimeCosts.stream().map(String::valueOf).collect(Collectors.toList());
    Files.write(filePath, lines);
  }

  public static List<Double> readFromFile(String fileName) throws IOException {
    Path filePath = Paths.get(fileName);
    List<String> lines = Files.readAllLines(filePath);
    return lines.stream().map(Double::valueOf).collect(Collectors.toList());
  }

  private List<List<String>> csvReader(String filePath) {
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
      e.printStackTrace();
    }
    System.out.println(data);
    return data;
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

  @Test
  public void test() {
    System.out.println("start");
    try {
      conn =
          new MultiConnection(
              new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
      conn.openSession();

      // 输出所有存储引擎
      String clusterInfo = conn.executeSql("SHOW CLUSTER INFO;").getResultInString(false, "");
      System.out.println(clusterInfo);
      // 获取当前JVM的Runtime实例
      Runtime runtime = Runtime.getRuntime();

      // 执行垃圾回收，尽量释放内存
      runtime.gc();

      // 获取执行语句前的内存使用情况
      long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();

      // 添加存储引擎
      System.out.println("start adding storage engine");
      long startTime = System.currentTimeMillis();
      Map<String, String> pgMap = new HashMap<>();
      pgMap.put("engine", "postgresql");
      pgMap.put("has_data", "true");
      pgMap.put("is_read_only", "true");
      pgMap.put("username", "postgres");
      pgMap.put("password", "postgres");
      pgMap = Collections.unmodifiableMap(pgMap);
      conn.addStorageEngine("127.0.0.1", 5432, StorageEngineType.relational, pgMap);
      Map<String, String> mongoMap = new HashMap<>();
      mongoMap.put("has_data", "true");
      mongoMap.put("is_read_only", "true");
      mongoMap.put("schema.sample.size", "1000");
      mongoMap.put("dummy.sample.size", "0");
      conn.addStorageEngine("127.0.0.1", 27017, StorageEngineType.mongodb, mongoMap);
      System.out.println(
          "end adding storage engine, time cost: "
              + (System.currentTimeMillis() - startTime)
              + "ms");

      // 输出所有存储引擎
      clusterInfo = conn.executeSql("SHOW CLUSTER INFO;").getResultInString(false, "");
      System.out.println(clusterInfo);
      // Long startTime;
      // 13有问题
      List<Integer> queryIds = Arrays.asList(1, 2, 3, 5, 6, 9, 10, 16, 17, 18, 19, 20);
      for (int queryId : queryIds) {
        // read from sql file
        String sqlString =
            readSqlFileAsString("src/test/resources/polybench/queries/q" + queryId + ".sql");

        // 开始 tpch 查询
        System.out.println("start tpch query " + queryId);
        startTime = System.currentTimeMillis();

        // 执行查询语句, split by ; 最后一句为执行结果
        SessionExecuteSqlResult result = null;
        String[] sqls = sqlString.split(";");
        for (String sql : sqls) {
          if (sql.trim().length() == 0) {
            continue;
          }
          sql += ";";
          result = conn.executeSql(sql);
          result.print(false, "");
        }
        // 再次执行垃圾回收
        runtime.gc();
        // 获取执行语句后的内存使用情况
        long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
        // 计算内存使用的变化
        long memoryUsed = usedMemoryAfter - usedMemoryBefore;
        // 输出内存使用情况
        System.out.println("Memory used by the statement: " + memoryUsed + " bytes");

        // 验证
        Long timeCost = System.currentTimeMillis() - startTime;
        System.out.println("end tpch query, time cost: " + timeCost + "ms");
        List<List<Object>> values = result.getValues();
        List<List<String>> answers =
            csvReader("src/test/resources/polybench/sf0.1/q" + queryId + ".csv");
        if (values.size() != answers.size()) {
          System.out.println("values.size() = " + values.size());
          System.out.println("answers.size() = " + answers.size());
          throw new RuntimeException("size not equal");
        }
        for (int i = 0; i < values.size(); i++) {
          if (values.get(i).size() != answers.get(i).size()) {
            System.out.println("values.get(i).size() = " + values.get(i).size());
            System.out.println("answers.get(i).size() = " + answers.get(i).size());
            throw new RuntimeException("size not equal");
          }
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
              String resultString =
                  new String((byte[]) values.get(i).get(j), StandardCharsets.UTF_8);
              String answerString = answers.get(i).get(j);
              if (!resultString.equals(answerString)) {
                System.out.println("Result string： " + resultString);
                System.out.println("Answer string: " + answerString);
              }
              assert resultString.equals(answerString);
            }
          }
        }
      }
      List<Double> avgTimeCosts = new ArrayList<>();
      for (int queryId : queryIds) {
        // read from sql file
        String sqlString =
            readSqlFileAsString("src/test/resources/polybench/queries/q" + queryId + ".sql");

        // 开始 tpch 查询
        System.out.println("start tpch query " + queryId);

        // 执行查询语句, split by ; 最后一句为执行结果
        SessionExecuteSqlResult result = null;
        String[] sqls = sqlString.split(";");
        List<Long> timeCosts = new ArrayList<>();
        for (int i = 0; i < 2; i++) { // TODO: 5
          startTime = System.currentTimeMillis();
          if (sqls.length == 1)
            // TODO: error
            System.out.println("wrong input");
          else result = conn.executeSql(sqls[sqls.length - 2] + ";");
          Long timeCost = System.currentTimeMillis() - startTime;
          timeCosts.add(timeCost);
          System.out.println("query " + queryId + ", time cost: " + timeCost + "ms");
        }
        Double averageTimeCost =
            timeCosts.stream().mapToLong(Long::longValue).average().getAsDouble();
        avgTimeCosts.add(averageTimeCost);
        System.out.println(
            "end tpch query " + queryId + ", average time cost: " + averageTimeCost + "ms");
      }
      String fileName = "src/test/resources/polybench/avgTimeCosts.txt";
      if (Files.exists(Paths.get(fileName))) { // 如果文件存在，即此次跑的是主分支代码，需要读取文件进行比较
        List<Double> newAvgTimeCosts = readFromFile(fileName); // 文件中存的是新分支的运行时间
        for (int i = 0; i < queryIds.size(); i++) {
          System.out.println(
              "query " + queryIds.get(i) + ", average time cost: " + avgTimeCosts.get(i) + "ms");
          System.out.println(
              "query "
                  + queryIds.get(i)
                  + ", new average time cost: "
                  + newAvgTimeCosts.get(i)
                  + "ms");
          // TODO 如果相差超过30%？，则报错
          if (Math.abs(newAvgTimeCosts.get(i) - avgTimeCosts.get(i)) > 0.3 * avgTimeCosts.get(i)) {
            System.out.println(
                "query "
                    + queryIds.get(i)
                    + ", new branch average time cost: "
                    + newAvgTimeCosts.get(i)
                    + "ms");
            System.out.println(
                "query " + queryIds.get(i) + ", average time cost: " + avgTimeCosts.get(i) + "ms");
            throw new RuntimeException("time cost not equal");
          }
        }
      } else {
        writeToFile(avgTimeCosts, fileName);
      }

      // 关闭会话
      conn.closeSession();
    } catch (SessionException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
