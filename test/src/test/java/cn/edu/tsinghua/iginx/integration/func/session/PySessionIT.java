package cn.edu.tsinghua.iginx.integration.func.session;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.expPort;
import static cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType.*;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PySessionIT {

  private static final Logger logger = LoggerFactory.getLogger(PySessionIT.class);

  // parameters to be flexibly configured by inheritance
  protected static MultiConnection session;
  private static String pythonCMD;
  private static boolean dummyNoData = true;
  // private static final TestDataSection baseDataSection = buildBaseDataSection();
  private static final long START_KEY = 0L;
  private static final long END_KEY = 4L;
  protected static boolean isForSession = true;
  protected static boolean isForSessionPool = false;

  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  static {
    String os = System.getProperty("os.name").toLowerCase();
    System.out.println(os);
    if (os.contains("windows")) {
      pythonCMD = "python";
    } else {
      pythonCMD = "python3"; // /opt/homebrew/anaconda3/envs/py310/bin/python
    }
  }

  private static boolean isAbleToDelete = true;

  public PySessionIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
  }

  //  private static TestDataSection buildBaseDataSection() {
  //    List<String> paths = Arrays.asList("a.a.a", "a.a.b", "a.b.b", "a.c.c");
  //    List<DataType> types =
  //        Arrays.asList(DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY);
  //    List<Map<String, String>> tagsList =
  //        Arrays.asList(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());
  //    List<Long> keys = Arrays.asList(0L, 1L, 2L, 3L);
  //    List<List<Object>> values =
  //        Arrays.asList(
  //            Arrays.asList(
  //                "a".getBytes(StandardCharsets.UTF_8),
  //                "b".getBytes(StandardCharsets.UTF_8),
  //                null,
  //                null),
  //            Arrays.asList(null, null, "b".getBytes(StandardCharsets.UTF_8), null),
  //            Arrays.asList(null, null, null, "c".getBytes(StandardCharsets.UTF_8)),
  //            Arrays.asList(
  //                "Q".getBytes(StandardCharsets.UTF_8),
  //                "W".getBytes(StandardCharsets.UTF_8),
  //                "E".getBytes(StandardCharsets.UTF_8),
  //                "R".getBytes(StandardCharsets.UTF_8)));
  //    return new TestDataSection(keys, types, paths, values, tagsList);
  //  }

  @BeforeClass
  public static void setUp() throws SessionException {
    // 清除历史数据
    logger.info("Clear all data before executing pysession tests.");
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
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
    }
    session.openSession();
    clearAllData(session);
    //    TestDataSection subBaseData = baseDataSection.getSubDataSectionWithKey(0, 4);
    //    insertData(subBaseData, Row);
    dummyNoData = false;
  }

  @Before
  public void insertBaseData() {
    List<String> result = new ArrayList<>();
    try {
      // 设置Python脚本路径
      String pythonScriptPath = "../session_py/tests/insertBaseDataset.py";

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
  }

  //  @Before
  //    public void insertBaseData() {
  //      TestDataSection subBaseData = baseDataSection.getSubDataSectionWithKey(0, 4);
  //      insertData(subBaseData, Row);
  //      dummyNoData = false; // insert base data using all types of insert API.
  //    }

  private static void insertData(TestDataSection data, InsertAPIType type) {
    switch (type) {
      case Row:
      case NonAlignedRow:
        Controller.writeRowsDataToDummy(
            session, data.getPaths(), data.getKeys(), data.getTypes(), data.getValues(), expPort);
        //        Controller.writeRowsDataToDummy(
        //                session, data.getPaths(), data.getKeys(), data.getTypes(),
        // data.getValues(), expPort);
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
    assertEquals(expected, result);
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
    assertEquals(expected, result);
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
    assertTrue(result.contains("path\ttype\t"));
    assertTrue(result.contains("b'a.a.a'\t\tb'BINARY'\t\t"));
    assertTrue(result.contains("b'a.a.b'\t\tb'BINARY'\t\t"));
    assertTrue(result.contains("b'a.b.b'\t\tb'BINARY'\t\t"));
    assertTrue(result.contains("b'a.c.c'\t\tb'BINARY'\t\t"));
    assertTrue(result.contains("a.a.a BINARY"));
    assertTrue(result.contains("a.a.b BINARY"));
    assertTrue(result.contains("a.b.b BINARY"));
    assertTrue(result.contains("a.c.c BINARY"));
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
    assertEquals(expected, result);
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
        Arrays.asList(
            "Time\tpath\tvalue\t",
            "3\tb'a.a.a'\tb'Q'\t",
            "3\tb'a.a.b'\tb'W'\t",
            "3\tb'a.b.b'\tb'E'\t",
            "3\tb'a.c.c'\tb'R'\t",
            "");
    assertEquals(expected, result);
  }

  @Test
  public void testDeleteColumn() {
    if (!isAbleToDelete) {
      return;
    }
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
    //    List<String> expected =
    //        Arrays.asList(
    //            "Time\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
    //            "0\tb'a'\tb'b'\tnull\tnull\t",
    //            "1\tnull\tnull\tb'b'\tnull\t",
    //            "2\tnull\tnull\tnull\tb'c'\t",
    //            "3\tb'Q'\tb'W'\tb'E'\tb'R'\t",
    //            "5\tnull\tnull\tnull\tb'b'\t",
    //            "6\tb'b'\tnull\tnull\tnull\t",
    //            "7\tb'R'\tb'E'\tnull\tb'Q'\t",
    //            "");
    List<String> expected =
        Arrays.asList(
            "Time\ta.a.a\ta.a.b\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\t",
            "2\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'R'\t",
            "5\tnull\tnull\tb'b'\t",
            "6\tb'b'\tnull\tnull\t",
            "7\tb'R'\tb'E'\tb'Q'\t",
            "");
    assertEquals(expected, result);
  }

  //  @Test
  public void testDeleteAll() {
    if (!isAbleToDelete) {
      return;
    }
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
    assertEquals(expected, result);
  }

  @Test
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
    // 如果是mongo或者pg
    if (result.size() > 0 && "This engine is already in the cluster.".equals(result.get(0))) {
      return;
    }
    assertEquals(result.size(), 12);
    assertTrue(result.get(1).contains("ip='127.0.0.1', port=5432, type=3"));
    assertFalse(result.get(1).contains("ip='127.0.0.1', port=27017, type=4"));
    assertFalse(result.get(4).contains("ip='127.0.0.1', port=5432, type=3"));
    assertFalse(result.get(1).contains("ip='127.0.0.1', port=27017, type=4"));
    assertTrue(result.get(7).contains("ip='127.0.0.1', port=5432, type=3"));
    assertTrue(result.get(7).contains("ip='127.0.0.1', port=27017, type=4"));
    assertFalse(result.get(10).contains("ip='127.0.0.1', port=5432, type=3"));
    assertFalse(result.get(10).contains("ip='127.0.0.1', port=27017, type=4"));
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
            "8\tnull\tb'a'\tb'b'\tnull\t",
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
            "8\tnull\tb'a'\tb'b'\tnull\tnull\t",
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
            "8\tnull\tb'a'\tb'b'\tnull\tnull\t",
            "9\tb'b'\tnull\tnull\tnull\tnull\t",
            "");
    assertEquals(expected, result);
  }

  @Test
  public void testDeleteRows() {
    if (!isAbleToDelete) {
      return;
    }
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
            "3\tb'Q'\tb'W'\tnull\tb'R'\t",
            "5\tnull\tnull\tnull\tb'b'\t",
            "6\tb'b'\tnull\tnull\tnull\t",
            "7\tb'R'\tb'E'\tnull\tb'Q'\t",
            "",
            "Time\ta.a.a\ta.a.b\ta.b.b\ta.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tnull\tb'R'\t",
            "5\tnull\tnull\tnull\tb'b'\t",
            "7\tb'R'\tb'E'\tnull\tb'Q'\t",
            "");
    assertEquals(expected, result);
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
    // assertEquals(expected, result);
  }

  @Test
  public void testLoadCSV() {
    if (!isAbleToDelete) {
      return;
    }
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
    assertEquals(expected, result);
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
    assertEquals(result.subList(result.size() - 3, result.size()), expected);
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
    // 验证写入stream是否成功
    String streamPathPrefix = System.getProperty("user.dir") + "/../generated/";
    List<String> streamFiles = Arrays.asList("a.a.a", "a.a.b", "a.b.b", "a.c.c");
    result.clear();
    for (String streamFile : streamFiles) {
      String streamPath = streamPathPrefix + streamFile;
      try (BufferedReader reader = new BufferedReader(new FileReader(streamPath))) {
        String line;
        while ((line = reader.readLine()) != null) {
          result.add(line);
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    List<String> expected = Arrays.asList("aQ", "bW", "bE", "cR");
    assertEquals(expected, result);
    // 验证写入csv文件是否成功
    String outputPath = System.getProperty("user.dir") + "/../generated/output.csv";
    expected =
        Arrays.asList(
            "key,a.a.a,a.a.b,a.b.b,a.c.c",
            "1970-01-01 08:00:00,a,b,None,None",
            "1970-01-01 08:00:01,None,None,b,None",
            "1970-01-01 08:00:02,None,None,None,c",
            "1970-01-01 08:00:03,Q,W,E,R");
    result.clear();
    try (BufferedReader reader = new BufferedReader(new FileReader(outputPath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.replace("1970-01-01 00:00", "1970-01-01 08:00"); // ubuntu上时间不一样
        result.add(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    assertEquals(expected, result);
  }

  public void clearInitialData() {
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
      }
      // 检查Python脚本是否正常终止
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        throw new RuntimeException("Python script terminated with non-zero exit code: " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("delete all");
  }

  @After
  public void clearData() {
    if (!isAbleToDelete) {
      return;
    }
    clearInitialData();
    // dummyNoData = true;
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    session.closeSession();
  }

  //    try {
  //      // 设置Python脚本路径
  //      String pythonScriptPath = "../session_py/tests/deleteAll.py";
  //
  //      // 创建ProcessBuilder以执行Python脚本
  //      ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);
  //
  //      // 启动进程并等待其终止
  //      Process process = pb.start();
  //      process.waitFor();
  //
  //      // 读取Python脚本的输出
  //      BufferedReader reader = new BufferedReader(new
  // InputStreamReader(process.getInputStream()));
  //      String line;
  //      while ((line = reader.readLine()) != null) {
  //        System.out.println(line);
  //      }
  //      // 检查Python脚本是否正常终止
  //      int exitCode = process.exitValue();
  //      if (exitCode != 0) {
  //        System.err.println("Python script terminated with non-zero exit code: " + exitCode);
  //      }
  //    } catch (IOException | InterruptedException e) {
  //      e.printStackTrace();
  //    }
  //    System.out.println("delete all");
  //  }
}
