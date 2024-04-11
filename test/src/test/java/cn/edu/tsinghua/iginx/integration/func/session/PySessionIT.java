package cn.edu.tsinghua.iginx.integration.func.session;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.utils.EnvUtils;
import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class PySessionIT {

  private static final Logger logger = LoggerFactory.getLogger(PySessionIT.class);

  // parameters to be flexibly configured by inheritance
  protected static MultiConnection session;
  private static String pythonCMD;
  private static final String PATH = String.join(File.separator, getHomePath(), "tests");
  protected static boolean isForSession = true;
  protected static boolean isForSessionPool = false;

  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  private static String getHomePath() {
    String iginxHomePath = EnvUtils.loadEnv(Constants.IGINX_HOME, System.getProperty("user.dir"));
    return Paths.get(iginxHomePath, "..", "session_py").toString();
  }

  static {
    String os = System.getProperty("os.name").toLowerCase();
    System.out.println(os);
    if (os.contains("windows")) {
      pythonCMD = "python";
    } else {
      pythonCMD = "python3"; // /opt/homebrew/anaconda3/envs/iginx/bin/python
    }
  }

  private static boolean isAbleToDelete = true;

  public PySessionIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    // 清除历史数据
    logger.info("Clear all data before executing pysession tests.");
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    logger.info("isAbleToDelete: " + isAbleToDelete);
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
  }

  private String runPythonScript(String fileName, String className)
      throws IOException, InterruptedException {
    try {
      PythonInterpreterConfig config =
          PythonInterpreterConfig.newBuilder()
              .setPythonExec(pythonCMD)
              .addPythonPaths(PATH)
              .build();
      PythonInterpreter interpreter = new PythonInterpreter(config);

      interpreter.exec("import " + fileName);
      interpreter.exec("t = " + fileName + "." + className + "()");

      String res = (String) interpreter.invokeMethod("t", "test");
      System.out.println(res);
      return res;
    } catch (RuntimeException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void insertBaseData() {
    try {
      String output = runPythonScript("insertBaseDataset", "InsertBaseDataset");
      System.out.println(output);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("insert");
  }

  @Test
  public void testAQuery() {
    String result = "";
    try {
      result = runPythonScript("query", "Query");
      System.out.println(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("query");
    String expected =
        String.join(
            "\n",
            "Time\ttest.a.a\ttest.a.b\ttest.b.b\ttest.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tb'R'\t",
            "   key test.a.a test.a.b test.b.b test.c.c",
            "0    0     b'a'     b'b'     None     None",
            "1    1     None     None     b'b'     None",
            "2    2     None     None     None     b'c'",
            "3    3     b'Q'     b'W'     b'E'     b'R'",
            "key\ttest.a.a\ttest.a.b\ttest.b.b\ttest.c.c\t",
            "0\t\tb'a'\t\tb'b'\t\tNone\t\tNone\t\t",
            "1\t\tNone\t\tNone\t\tb'b'\t\tNone\t\t",
            "2\t\tNone\t\tNone\t\tNone\t\tb'c'\t\t",
            "3\t\tb'Q'\t\tb'W'\t\tb'E'\t\tb'R'\t\t",
            "",
            "replicaNum: 1",
            "");
    assertEquals(expected, result);
  }

  @Test
  public void testDownSampleQuery() {
    String result = "";
    try {
      // 设置Python脚本路径
      result = runPythonScript("downsampleQuery", "DownsampleQuery");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("downsample query");
    // 检查Python脚本的输出是否符合预期
    String expected =
        String.join(
            "\n",
            "Time\tcount(test.a.a)\tcount(test.a.b)\tcount(test.b.b)\tcount(test.c.c)\t",
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
    String result = "";
    try {
      result = runPythonScript("showColumns", "ShowColumns");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("show columns query");
    // 检查Python脚本的输出是否符合预期
    //    assertTrue(result.contains("path\ttype\t"));
    //    assertTrue(result.contains("b'test.a.a'\t\tb'BINARY'\t\t"));
    //    assertTrue(result.contains("b'test.a.b'\t\tb'BINARY'\t\t"));
    //    assertTrue(result.contains("b'test.b.b'\t\tb'BINARY'\t\t"));
    //    assertTrue(result.contains("b'test.c.c'\t\tb'BINARY'\t\t"));
    //    assertTrue(result.contains("test.a.a BINARY"));
    //    assertTrue(result.contains("test.a.b BINARY"));
    //    assertTrue(result.contains("test.b.b BINARY"));
    //    assertTrue(result.contains("test.c.c BINARY"));
  }

  @Test
  public void testAggregateQuery() {
    String result = "";
    try {
      result = runPythonScript("aggregateQuery", "AggregateQuery");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("aggregate query");
    // 检查Python脚本的输出是否符合预期
    String expected =
        String.join(
            "\n",
            "COUNT(count(test.a.a))\tCOUNT(count(test.a.b))\tCOUNT(count(test.b.b))\tCOUNT(count(test.c.c))\t",
            "2\t2\t2\t2\t",
            "");
    assertEquals(expected, result);
  }

  @Test
  public void testLastQuery() {
    String result = "";
    try {
      result = runPythonScript("lastQuery", "LastQuery");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("last query");
    // 检查Python脚本的输出是否符合预期
    String expected =
        String.join(
            "\n",
            "Time\tpath\tvalue\t",
            "3\tb'test.a.a'\tb'Q'\t",
            "3\tb'test.a.b'\tb'W'\t",
            "3\tb'test.b.b'\tb'E'\t",
            "3\tb'test.c.c'\tb'R'\t",
            "");
    assertEquals(expected, result);
  }

  @Test
  public void testDeleteColumn() {
    if (!isAbleToDelete) {
      return;
    }
    String result = "";
    try {
      result = runPythonScript("deleteColumn", "DeleteColumn");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("delete column query");
    String expected =
        String.join(
            "\n",
            "Time\ttest.a.a\ttest.a.b\ttest.c.c\t",
            "0\tb'a'\tb'b'\tnull\t",
            "2\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'R'\t",
            "5\tnull\tnull\tb'b'\t",
            "6\tb'b'\tnull\tnull\t",
            "7\tb'R'\tb'E'\tb'Q'\t",
            "");
    assertEquals(expected, result);
  }

  @Test
  public void testAddStorageEngine() {
    String output = "";
    try {
      output = runPythonScript("addStorageEngine", "AddStorageEngine");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("add and delete storage engine");
    String[] lines = output.split("\n");
    List<String> result = Arrays.asList(lines);
    System.out.println(result);
    // 如果是pg
    if (result.size() > 0
        && "The storage engine has been added, please delete it first".equals(result.get(0))) {
      return;
    }
    assertEquals(result.size(), 12);
    assertTrue(result.get(1).contains("ip='127.0.0.1', port=5432, type=3"));
    assertFalse(result.get(4).contains("ip='127.0.0.1', port=5432, type=3"));
    assertTrue(result.get(7).contains("ip='127.0.0.1', port=5432, type=3"));
    assertFalse(result.get(10).contains("ip='127.0.0.1', port=5432, type=3"));
  }

  @Test
  public void testInsert() {
    String result = "";
    try {
      result = runPythonScript("insert", "Insert");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("insert");
    // 检查Python脚本的输出是否符合预期
    String expected =
        String.join(
            "\n",
            "Time\ttest.a.a\ttest.a.b\ttest.b.b\ttest.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tb'R'\t",
            "5\tnull\tnull\tb'a'\tb'b'\t",
            "6\tb'b'\tnull\tnull\tnull\t",
            "7\tb'R'\tb'E'\tb'W'\tb'Q'\t",
            "Time\ttest.a.a\ttest.a.b\ttest.b.b\ttest.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tb'R'\t",
            "5\tnull\tnull\tb'a'\tb'b'\t",
            "6\tb'b'\tnull\tnull\tnull\t",
            "7\tb'R'\tb'E'\tb'W'\tb'Q'\t",
            "8\tnull\tb'a'\tb'b'\tnull\t",
            "9\tb'b'\tnull\tnull\tnull\t",
            "Time\ttest.a.a\ttest.a.b\ttest.b.b\ttest.b.c\ttest.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\tnull\t",
            "1\tnull\tnull\tb'b'\tnull\tnull\t",
            "2\tnull\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tb'E'\tnull\tb'R'\t",
            "5\tnull\tnull\tb'a'\tnull\tb'b'\t",
            "6\tb'b'\tnull\tnull\t1\tnull\t",
            "7\tb'R'\tb'E'\tb'W'\tnull\tb'Q'\t",
            "8\tnull\tb'a'\tb'b'\tnull\tnull\t",
            "9\tb'b'\tnull\tnull\tnull\tnull\t",
            "Time\ttest.a.a\ttest.a.b\ttest.b.b\ttest.b.c\ttest.c.c\t",
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
  public void testDeleteRow() {
    if (!isAbleToDelete) {
      return;
    }
    String result = "";
    try {
      result = runPythonScript("deleteRow", "DeleteRow");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("delete row");
    // 检查Python脚本的输出是否符合预期
    String expected =
        String.join(
            "\n",
            "Time\ttest.a.a\ttest.a.b\ttest.b.b\ttest.c.c\t",
            "0\tb'a'\tb'b'\tnull\tnull\t",
            "2\tnull\tnull\tnull\tb'c'\t",
            "3\tb'Q'\tb'W'\tnull\tb'R'\t",
            "5\tnull\tnull\tnull\tb'b'\t",
            "6\tb'b'\tnull\tnull\tnull\t",
            "7\tb'R'\tb'E'\tnull\tb'Q'\t",
            "Time\ttest.a.a\ttest.a.b\ttest.b.b\ttest.c.c\t",
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
    String result = "";
    try {
      result = runPythonScript("getDebugInfo", "GetDebugInfo");
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
    String result = "";
    try {
      result = runPythonScript("loadCSV", "LoadCSV");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("load csv without header");
    // 检查Python脚本的输出是否符合预期
    String expected =
        String.join(
            "\n",
            "LoadCSVResp(status=Status(code=200, message=None, subStatus=None), columns=['test.a.a', 'test.a.b', 'test.b.b', 'test.c.c'], recordsNum=4, parseErrorMsg=None)",
            "key\ttest.a.a\ttest.a.b\ttest.b.b\ttest.c.c\t",
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
    String result = "";
    try {
      result = runPythonScript("loadDirectory", "LoadDirectory");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("load csv without header");
    // 检查Python脚本的输出是否符合预期
    List<String> expected = Arrays.asList("key\tdir.a\tdir.b\t", "0\t\tb'1'\t\tb'4'\t\t");
    // keep result[-3:] to check if the data is loaded successfully
    System.out.println(result);
    String[] lines = result.split("\n");
    List<String> resultLines = Arrays.asList(lines);
    System.out.println(resultLines);
    assertTrue(resultLines.size() >= 2);
    assertEquals(resultLines.subList(resultLines.size() - 2, resultLines.size()), expected);
  }

  @Test
  public void testExport() {
    List<String> result = new ArrayList<>();
    try {
      String tmp = runPythonScript("exportToFile", "ExportToFile");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("load csv without header");
    // 验证写入stream是否成功
    String streamPathPrefix = System.getProperty("user.dir") + "/../generated/";
    List<String> streamFiles = Arrays.asList("test.a.a", "test.a.b", "test.b.b", "test.c.c");
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
            "key,test.a.a,test.a.b,test.b.b,test.c.c",
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

  @After
  public void clearData() {
    if (!isAbleToDelete) {
      return;
    }
    try {
      String output = runPythonScript("deleteAll", "DeleteAll");
      System.out.println(output);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("delete all");
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    session.closeSession();
  }
}
