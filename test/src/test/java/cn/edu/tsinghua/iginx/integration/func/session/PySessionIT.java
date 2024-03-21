package cn.edu.tsinghua.iginx.integration.func.session;

import static cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PySessionIT {

  private static final Logger logger = LoggerFactory.getLogger(PySessionIT.class);

  // parameters to be flexibly configured by inheritance
  protected static MultiConnection session;
  private static boolean dummyNoData = true;
  private String pythonCMD = "python3";

  // host info
  protected String defaultTestHost = "127.0.0.1";
  protected int defaultTestPort = 6888;
  protected String defaultTestUser = "root";
  protected String defaultTestPass = "root";
  private static final TestDataSection baseDataSection = buildBaseDataSection();

  protected boolean isAbleToDelete;

  public PySessionIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
  }

  private static TestDataSection buildBaseDataSection() {
    List<String> paths = Arrays.asList("a.a.a", "a.a.b", "a.b.b", "a.c.c");
    List<DataType> types =
        Arrays.asList(DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY);
    List<Map<String, String>> tagsList =
        Arrays.asList(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());
    // keys = [0, 1, 2, 3]
    List<Long> keys = IntStream.range(0, 4).mapToObj(i -> (long) i).collect(Collectors.toList());
    /*
     values = [
        ['a', 'b', None, None],
        [None, None, 'b', None],
        [None, None, None, 'c'],
        ['Q', 'W', 'E', 'R'],
    ]
    * */
    List<List<Object>> values =
        Arrays.asList(
            Arrays.asList(
                "a".getBytes(StandardCharsets.UTF_8),
                "b".getBytes(StandardCharsets.UTF_8),
                null,
                null),
            Arrays.asList(null, null, "b".getBytes(StandardCharsets.UTF_8), null),
            Arrays.asList(null, null, null, "c".getBytes(StandardCharsets.UTF_8)),
            Arrays.asList(
                "Q".getBytes(StandardCharsets.UTF_8),
                "W".getBytes(StandardCharsets.UTF_8),
                "E".getBytes(StandardCharsets.UTF_8),
                "R".getBytes(StandardCharsets.UTF_8)));
    return new TestDataSection(keys, types, paths, values, tagsList);
  }

  private void insertData(TestDataSection data) {
    Controller.writeRowsData(
        session,
        data.getPaths(),
        data.getKeys(),
        data.getTypes(),
        data.getValues(),
        data.getTagsList(),
        Row,
        dummyNoData);
  }

  @Before
  public void setUp() {
    try {
      session =
          new MultiConnection(
              new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
      session.openSession();
      long start = 0, end = 4;
      TestDataSection subBaseData = baseDataSection.getSubDataSectionWithKey(start, end);
      insertData(subBaseData);
      dummyNoData = false;
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Test
  public void testAQuery() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/query.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("query");
    List<String> expected =
        Arrays.asList(
            "[IginxInfo(id=0, ip='0.0.0.0', port=6888)]",
            "[StorageEngineInfo(id=0, ip='127.0.0.1', port=6667, type=0, schemaPrefix='null', dataPrefix='null')]",
            "[MetaStorageInfo(ip='127.0.0.1', port=2181, type='zookeeper')]",
            "Time\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tb'R'\t",
            "",
            "   key a.a.a a.a.b a.b.b a.c.c",
            "0    0  b'a'  b'b'  None  None",
            "1    1  None  None  b'b'  None",
            "2    2  None  None  None  b'c'",
            "3    3  b'Q'  b'W'  b'E'  b'R'",
            "key\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
            "0\t\tb'a'\t\tb'b'\t\tNone\t\tNone\t\t",
            "1\t\tNone\t\tNone\t\tb'b'\t\tNone\t\t",
            "2\t\tNone\t\tNone\t\tNone\t\tb'c'\t\t",
            "3\t\tb'Q'\t\tb'W'\t\tb'E'\t\tb'R'\t\t",
            "",
            "replicaNum: 1");
    // assertEquals(result, expected);
  }

  @Test
  public void testDownSampleQuery() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/downsampleQuery.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("downsample query");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "Time\tcount(a.a.a)\tcount(a.a.b)\tcount(a.b.b)\tcount(a.c.c)\t",
            "0\t1\t1\t1\t1\t",
            "3\t1\t1\t1\t1\t",
            "");
    // assertEquals(result, expected);
  }

  // 用两种方式测试查询列信息：
  // 1. 直接执行`show columns;` sql
  // 2. 使用 list_time_series() 接口查询时间序列
  @Test
  public void testShowColumnsQuery() {
    List<String> result = new ArrayList<>();
    try {
      logger.info("111");
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/showColumns.py";
      // 确认文件存在
      boolean fileExists = Files.exists(Paths.get(pythonScriptPath));
      logger.info("file exists: " + fileExists);
      logger.info("222");
      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);
      logger.info("333");
      // 启动进程并等待其终止
      Process process = pb.start();
      logger.info("444");
      process.waitFor();
      logger.info("555");
      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      logger.info("666");
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        logger.info(line);
        result.add(line);
      }
      logger.info("777");
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("show columns query");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "path\ttype\t",
            "b'a.a.a'\t\tb'BINARY'\t\t",
            "b'a.a.b'\t\tb'BINARY'\t\t",
            "b'a.b.b'\t\tb'BINARY'\t\t",
            "b'a.c.c'\t\tb'BINARY'\t\t",
            "",
            "a.a.a BINARY",
            "a.a.b BINARY",
            "a.b.b BINARY",
            "a.c.c BINARY");
    // assertEquals(result, expected);
  }

  @Test
  public void testAggregateQuery() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/aggregateQuery.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("aggregate query");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "COUNT(count(a.a.a))\tCOUNT(count(a.a.b))\tCOUNT(count(a.b.b))\tCOUNT(count(a.c.c))\t",
            "2\t2\t2\t2\t",
            "",
            "   COUNT(count(a.a.a))  COUNT(count(a.a.b))  COUNT(count(a.b.b))  COUNT(count(a.c.c))",
            "0                    2                    2                    2                    2");
    // assertEquals(result, expected);
  }

  @Test
  public void testLastQuery() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/lastQuery.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("last query");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList("Time\tpath\tvalue\t", "3\tb'a.a.a'\tb'Q'\t", "3\tb'a.a.b'\tb'W'\t", "");
    // assertEquals(result, expected);
  }

  @Test
  public void testDeleteColumn() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/deleteColumn.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("delete column query");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "Time\ta.a.a\ta.a.b\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\t",
            "2\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'R'\t",
            "");
    // assertEquals(result, expected);
  }

  @Test
  public void testDeleteAll() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/deleteAll.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("delete all");
    // 检查Python脚本的输出是否符合预期
    List<String> expected = Arrays.asList("Time\t", "");
    // assertEquals(result, expected);
  }

  // @Test
  public void testAddStorageEngine() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/addStorageEngine.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("add and delete storage engine");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "[IginxInfo(id=0, ip='0.0.0.0', port=6888)]",
            "[StorageEngineInfo(id=0, ip='127.0.0.1', port=6667, type=0, schemaPrefix='null', dataPrefix='null'), StorageEngineInfo(id=1, ip='127.0.0.1', port=5432, type=3, schemaPrefix='null', dataPrefix='null')]",
            "[MetaStorageInfo(ip='127.0.0.1', port=2181, type='zookeeper')]",
            "[IginxInfo(id=0, ip='0.0.0.0', port=6888)]",
            "[StorageEngineInfo(id=0, ip='127.0.0.1', port=6667, type=0, schemaPrefix='null', dataPrefix='null')]",
            "[MetaStorageInfo(ip='127.0.0.1', port=2181, type='zookeeper')]",
            "[IginxInfo(id=0, ip='0.0.0.0', port=6888)]",
            "[StorageEngineInfo(id=0, ip='127.0.0.1', port=6667, type=0, schemaPrefix='null', dataPrefix='null'), StorageEngineInfo(id=1, ip='127.0.0.1', port=5432, type=3, schemaPrefix='null', dataPrefix='null'), StorageEngineInfo(id=3, ip='127.0.0.1', port=27017, type=4, schemaPrefix='null', dataPrefix='null')]",
            "[MetaStorageInfo(ip='127.0.0.1', port=2181, type='zookeeper')]",
            "[IginxInfo(id=0, ip='0.0.0.0', port=6888)]",
            "[StorageEngineInfo(id=0, ip='127.0.0.1', port=6667, type=0, schemaPrefix='null', dataPrefix='null')]",
            "[MetaStorageInfo(ip='127.0.0.1', port=2181, type='zookeeper')]");
    // 通过正则匹配将加入的storage engine的id替换为1
    for (int i = 0; i < result.size(); i++) {
      String replacedString =
          result
              .get(i)
              .replaceAll(
                  "StorageEngineInfo\\(id=[1-9][0-9]*, ip='127.0.0.1', port=5432",
                  "StorageEngineInfo(id=1, ip='127.0.0.1', port=5432")
              .replaceAll(
                  "StorageEngineInfo\\(id=[1-9][0-9]*, ip='127.0.0.1', port=27017",
                  "StorageEngineInfo(id=3, ip='127.0.0.1', port=27017");
      result.set(i, replacedString);
    }
    // assertEquals(result, expected);
  }

  @Test
  public void testInsert() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/insert.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("insert");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "Time\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tb'R'\t",
            "5\tnull\tnull\tb'a'\tb'b'\t",
            "6\tb'b'\tnull\tnull\tnull\t",
            "7\tb'R'\tb'E'\tb'W'\tb'Q'\t",
            "",
            "Time\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tb'R'\t",
            "5\tnull\tnull\tb'a'\tb'b'\t",
            "6\tb'b'\tnull\tnull\tnull\t",
            "7\tb'R'\tb'E'\tb'W'\tb'Q'\t",
            "8\tnull\tnull\tb'b'\tnull\t",
            "9\tb'b'\tnull\tnull\tnull\t",
            "",
            "Time\ta.a.a\ta.a.b\ta.b.b\ta.b.c\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\tnull\t",
            "2\tnull\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tnull\tb'R'\t",
            "5\tnull\tnull\tb'a'\tnull\tb'b'\t",
            "6\tb'b'\tnull\tnull\t1\tnull\t",
            "7\tb'R'\tb'E'\tb'W'\tnull\tb'Q'\t",
            "8\tnull\tnull\tb'b'\tnull\tnull\t",
            "9\tb'b'\tnull\tnull\tnull\tnull\t",
            "",
            "Time\ta.a.a\ta.a.b\ta.b.b\ta.b.c\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\tnull\t",
            "2\tnull\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tnull\tb'R'\t",
            "5\tnull\tnull\tb'a'\t1\tb'b'\t",
            "6\tb'b'\tnull\tnull\t1\tnull\t",
            "7\tb'R'\tb'E'\tb'W'\tnull\tb'Q'\t",
            "8\tnull\tnull\tb'b'\tnull\tnull\t",
            "9\tb'b'\tnull\tnull\tnull\tnull\t",
            "");
    // assertEquals(result, expected);
  }

  @Test
  public void testDeleteRows() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/deleteRow.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("delete row");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "Time\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tb'R'\t",
            "",
            "Time\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tnull\tnull\tb'E'\tb'R'\t",
            "");
    // assertEquals(result, expected);
  }

  @Test
  public void testDebugInfo() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/getDebugInfo.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("get debug info");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "{\"fragments\":[{\"endKey\":9223372036854775807,\"endTs\":\"a.a.a\",\"setEndKey\":true,\"setEndTs\":true,\"setStartKey\":true,\"setStartTs\":false,\"setStorageUnitId\":true,\"startKey\":0,\"storageUnitId\":\"unit0000000002\"},{\"endKey\":9223372036854775807,\"endTs\":\"a.c.c\",\"setEndKey\":true,\"setEndTs\":true,\"setStartKey\":true,\"setStartTs\":true,\"setStorageUnitId\":true,\"startKey\":0,\"startTs\":\"a.a.a\",\"storageUnitId\":\"unit0000000000\"},{\"endKey\":9223372036854775807,\"setEndKey\":true,\"setEndTs\":false,\"setStartKey\":true,\"setStartTs\":true,\"setStorageUnitId\":true,\"startKey\":0,\"startTs\":\"a.c.c\",\"storageUnitId\":\"unit0000000001\"}],\"fragmentsIterator\":{},\"fragmentsSize\":3,\"setFragments\":true,\"setStorageUnits\":true,\"setStorages\":true,\"storageUnits\":[{\"id\":\"unit0000000000\",\"masterId\":\"unit0000000000\",\"setId\":true,\"setMasterId\":true,\"setStorageId\":true,\"storageId\":0},{\"id\":\"unit0000000001\",\"masterId\":\"unit0000000001\",\"setId\":true,\"setMasterId\":true,\"setStorageId\":true,\"storageId\":0},{\"id\":\"unit0000000002\",\"masterId\":\"unit0000000002\",\"setId\":true,\"setMasterId\":true,\"setStorageId\":true,\"storageId\":0}],\"storageUnitsIterator\":{},\"storageUnitsSize\":3,\"storages\":[{\"id\":0,\"ip\":\"127.0.0.1\",\"port\":6667,\"setId\":true,\"setIp\":true,\"setPort\":true,\"setType\":true,\"type\":\"iotdb12\"}],\"storagesIterator\":{},\"storagesSize\":1}");
    // assertEquals(result, expected);
  }

  @Test
  public void testLoadCSV() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/loadCSV.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("load csv without header");
    // 检查Python脚本的输出是否符合预期
    List<String> expected =
        Arrays.asList(
            "LoadCSVResp(status=Status(code=200, message=None, subStatus=None), columns=['a.a.a', 'a.a.b', 'a.b.b', 'a.c.c'], recordsNum=4, parseErrorMsg=None)",
            "key\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
            "0\t\tb'a'\t\tb'b'\t\tNone\t\tNone\t\t",
            "1\t\tNone\t\tNone\t\tb'b'\t\tNone\t\t",
            "2\t\tNone\t\tNone\t\tNone\t\tb'c'\t\t",
            "3\t\tb'Q'\t\tb'W'\t\tb'E'\t\tb'R'\t\t",
            "4\t\tb'a'\t\tb'b'\t\tb''\t\tb''\t\t",
            "5\t\tb''\t\tb''\t\tb'b'\t\tb''\t\t",
            "6\t\tb''\t\tb''\t\tb''\t\tb'c'\t\t",
            "7\t\tb'Q'\t\tb'W'\t\tb'E'\t\tb'R'\t\t",
            "");
    // assertEquals(result, expected);
  }

  @Test
  public void testLoadDirectory() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/loadDirectory.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("load csv without header");
    // 检查Python脚本的输出是否符合预期
    List<String> expected = Arrays.asList("key\tdir.a\tdir.b\t", "0\t\tb'1'\t\tb'4'\t\t", "");
    // keep result[-3:] to check if the data is loaded successfully
    // assertEquals(result.subList(result.size() - 3, result.size()), expected);
  }

  @Test
  public void testExport() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/exportToFile.py";

      // 创建ProcessBuilder以执行Python脚本
      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

      // 启动进程并等待其终止
      Process process = pb.start();
      process.waitFor();

      // 读取Python脚本的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        result.add(line);
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        for (int i = 0; i < result.size(); i++) {
          logger.info(result.get(i));
        }
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("load csv without header");
    //    // 检查Python脚本的输出是否符合预期
    //    List<String> expected =
    //            Arrays.asList("");
    //    // keep result[-3:] to check if the data is loaded successfully
    //    assertEquals(result, expected);
  }

  @After
  public void tearDown() {
    try {
      clearData();
      session.closeSession();
    } catch (SessionException e) {
      logger.error(e.getMessage());
    }
  }

  protected void clearData() throws SessionException {
    if (session.isClosed()) {
      session =
          new MultiConnection(
              new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
      session.openSession();
    }

    Controller.clearData(session);
  }
}
