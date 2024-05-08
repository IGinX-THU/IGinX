package cn.edu.tsinghua.iginx.integration.func.session;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
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

  protected static MultiConnection session;
  private static final String PATH =
      Paths.get(
              EnvUtils.loadEnv(Constants.IGINX_HOME, System.getProperty("user.dir")),
              "src",
              "test",
              "resources",
              "pySessionIT")
          .toString();
  protected static boolean isForSession = true;
  protected static boolean isForSessionPool = false;

  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static String pythonCMD = config.getPythonCMD();

  private static boolean isAbleToDelete = true;
  private static PythonInterpreter interpreter;

  public PySessionIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    PythonInterpreterConfig config =
        PythonInterpreterConfig.newBuilder().setPythonExec(pythonCMD).addPythonPaths(PATH).build();
    interpreter = new PythonInterpreter(config);
    interpreter.exec("import tests");
    interpreter.exec("t = tests.Tests()");
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

  private String runPythonScript(String functionName) throws IOException, InterruptedException {
    try {
      String res = (String) interpreter.invokeMethod("t", functionName);
      return res;
    } catch (RuntimeException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void insertBaseData() {
    try {
      logger.info("Insert base data before executing pysession tests.");
      String output = runPythonScript("insertBaseDataset");
      logger.info(output);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAQuery() {
    String result = "";
    try {
      logger.info("Test A query");
      result = runPythonScript("query");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    String expected =
        "   key test.a.a test.a.b test.b.b test.c.c\n"
            + "0    0     b'a'     b'b'     None     None\n"
            + "1    1     None     None     b'b'     None\n"
            + "2    2     None     None     None     b'c'\n"
            + "3    3     b'Q'     b'W'     b'E'     b'R'\n"
            + "key    test.a.a    test.a.b    test.b.b    test.c.c    \n"
            + "0        b'a'        b'b'        None        None        \n"
            + "1        None        None        b'b'        None        \n"
            + "2        None        None        None        b'c'        \n"
            + "3        b'Q'        b'W'        b'E'        b'R'        \n"
            + "\n"
            + "replicaNum: 1\n";
    assertEquals(expected, result);
  }

  @Test
  public void testDownSampleQuery() {
    String result = "";
    try {
      // 设置Python脚本路径
      logger.info("Test downsample query");
      result = runPythonScript("downsampleQuery");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 检查Python脚本的输出是否符合预期
    String expected =
        "   key  count(test.a.a)  count(test.a.b)  count(test.b.b)  count(test.c.c)\n"
            + "0    0                1                1                1                1\n"
            + "1    3                1                1                1                1\n";
    assertEquals(expected, result);
  }

  // 用两种方式测试查询列信息：
  // 1. 直接执行`show columns;` sql
  // 2. 使用 list_time_series() 接口查询时间序列
  @Test
  public void testShowColumnsQuery() {
    String result = "";
    try {
      logger.info("Test show columns query");
      result = runPythonScript("showColumns");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 检查Python脚本的输出是否符合预期
    assertTrue(result.contains("path    type    "));
    assertTrue(result.contains("b'test.a.a'        b'BINARY'        "));
    assertTrue(result.contains("b'test.a.b'        b'BINARY'        "));
    assertTrue(result.contains("b'test.b.b'        b'BINARY'        "));
    assertTrue(result.contains("b'test.c.c'        b'BINARY'        "));
    assertTrue(result.contains("test.a.a BINARY"));
    assertTrue(result.contains("test.a.b BINARY"));
    assertTrue(result.contains("test.b.b BINARY"));
    assertTrue(result.contains("test.c.c BINARY"));
  }

  @Test
  public void testAggregateQuery() {
    String result = "";
    try {
      logger.info("Test aggregate query");
      result = runPythonScript("aggregateQuery");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 检查Python脚本的输出是否符合预期
    String expected =
        "[   COUNT(count(test.a.a))  COUNT(count(test.a.b))  COUNT(count(test.b.b))  \\\n"
            + "0                       2                       2                       2   \n"
            + "\n"
            + "   COUNT(count(test.c.c))  \n"
            + "0                       2  ]\n";
    assertEquals(expected, result);
  }

  @Test
  public void testLastQuery() {
    String result = "";
    try {
      logger.info("Test last query");
      result = runPythonScript("lastQuery");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 检查Python脚本的输出是否符合预期
    String expected =
        "   key         path value\n"
            + "0    3  b'test.a.a'  b'Q'\n"
            + "1    3  b'test.a.b'  b'W'\n"
            + "2    3  b'test.b.b'  b'E'\n"
            + "3    3  b'test.c.c'  b'R'\n";
    assertEquals(expected, result);
  }

  @Test
  public void testDeleteColumn() {
    if (!isAbleToDelete) {
      return;
    }
    String result = "";
    try {
      logger.info("Test delete column query");
      result = runPythonScript("deleteColumn");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    String expected =
        "   key test.a.a test.a.b test.c.c\n"
            + "0    0     b'a'     b'b'     None\n"
            + "1    2     None     None     b'c'\n"
            + "2    3     b'Q'     b'W'     b'R'\n"
            + "3    5     None     None     b'b'\n"
            + "4    6     b'b'     None     None\n"
            + "5    7     b'R'     b'E'     b'Q'\n";
    assertEquals(expected, result);
  }

  // TODO this test needs correction
  // @Test
  public void testAddStorageEngine() {
    String output = "";
    try {
      logger.info("add storage engine");
      output = runPythonScript("addStorageEngine");
      logger.info(output);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    String[] lines = output.split("\n");
    List<String> result = Arrays.asList(lines);
    // 如果是该端口已被占用
    if (result.size() > 0
        && "The storage engine has been added, please delete it first".equals(result.get(0))) {
      return;
    }
    assertEquals(result.size(), 12);
    assertTrue(result.get(1).contains("ip='127.0.0.1', port=6670, type='parquet'"));
    assertFalse(result.get(1).contains("ip='127.0.0.1', port=6671, type='filesystem'"));
    assertFalse(result.get(4).contains("ip='127.0.0.1', port=6670, type='parquet'"));
    assertFalse(result.get(4).contains("ip='127.0.0.1', port=6671, type='filesystem'"));
    assertTrue(result.get(7).contains("ip='127.0.0.1', port=6670, type='parquet'"));
    assertTrue(result.get(7).contains("ip='127.0.0.1', port=6671, type='filesystem'"));
    assertFalse(result.get(10).contains("ip='127.0.0.1', port=6670, type='parquet'"));
    assertFalse(result.get(10).contains("ip='127.0.0.1', port=6671, type='filesystem'"));
  }

  @Test
  public void testInsert() {
    String result = "";
    try {
      logger.info("insert data");
      result = runPythonScript("insert");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 检查Python脚本的输出是否符合预期
    String expected =
        "   key test.a.a test.a.b test.b.b test.c.c\n"
            + "0    0     b'a'     b'b'     None     None\n"
            + "1    1     None     None     b'b'     None\n"
            + "2    2     None     None     None     b'c'\n"
            + "3    3     b'Q'     b'W'     b'E'     b'R'\n"
            + "4    5     None     None     b'a'     b'b'\n"
            + "5    6     b'b'     None     None     None\n"
            + "6    7     b'R'     b'E'     b'W'     b'Q'\n"
            + "   key test.a.a test.a.b test.b.b test.c.c\n"
            + "0    0     b'a'     b'b'     None     None\n"
            + "1    1     None     None     b'b'     None\n"
            + "2    2     None     None     None     b'c'\n"
            + "3    3     b'Q'     b'W'     b'E'     b'R'\n"
            + "4    5     None     None     b'a'     b'b'\n"
            + "5    6     b'b'     None     None     None\n"
            + "6    7     b'R'     b'E'     b'W'     b'Q'\n"
            + "7    8     None     b'a'     b'b'     None\n"
            + "8    9     b'b'     None     None     None\n"
            + "   key test.a.a test.a.b test.b.b  test.b.c test.c.c\n"
            + "0    0     b'a'     b'b'     None       NaN     None\n"
            + "1    1     None     None     b'b'       NaN     None\n"
            + "2    2     None     None     None       NaN     b'c'\n"
            + "3    3     b'Q'     b'W'     b'E'       NaN     b'R'\n"
            + "4    5     None     None     b'a'       NaN     b'b'\n"
            + "5    6     b'b'     None     None       1.0     None\n"
            + "6    7     b'R'     b'E'     b'W'       NaN     b'Q'\n"
            + "7    8     None     b'a'     b'b'       NaN     None\n"
            + "8    9     b'b'     None     None       NaN     None\n"
            + "   key test.a.a test.a.b test.b.b  test.b.c test.c.c\n"
            + "0    0     b'a'     b'b'     None       NaN     None\n"
            + "1    1     None     None     b'b'       NaN     None\n"
            + "2    2     None     None     None       NaN     b'c'\n"
            + "3    3     b'Q'     b'W'     b'E'       NaN     b'R'\n"
            + "4    5     None     None     b'a'       1.0     b'b'\n"
            + "5    6     b'b'     None     None       1.0     None\n"
            + "6    7     b'R'     b'E'     b'W'       NaN     b'Q'\n"
            + "7    8     None     b'a'     b'b'       NaN     None\n"
            + "8    9     b'b'     None     None       NaN     None\n";

    assertEquals(expected, result);
  }

  @Test
  public void testDeleteRow() {
    if (!isAbleToDelete) {
      return;
    }
    String result = "";
    try {
      logger.info("delete row");
      result = runPythonScript("deleteRow");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 检查Python脚本的输出是否符合预期
    String expected =
        "   key test.a.a test.a.b test.b.b test.c.c\n"
            + "0    0     b'a'     b'b'     None     None\n"
            + "1    2     None     None     None     b'c'\n"
            + "2    3     b'Q'     b'W'     None     b'R'\n"
            + "3    5     None     None     None     b'b'\n"
            + "4    6     b'b'     None     None     None\n"
            + "5    7     b'R'     b'E'     None     b'Q'\n"
            + "   key test.a.a test.a.b test.b.b test.c.c\n"
            + "0    0     b'a'     b'b'     None     None\n"
            + "1    2     None     None     None     b'c'\n"
            + "2    3     b'Q'     b'W'     None     b'R'\n"
            + "3    5     None     None     None     b'b'\n"
            + "4    7     b'R'     b'E'     None     b'Q'\n";
    assertEquals(expected, result);
  }

  @Test
  public void testDebugInfo() {
    String result = "";
    try {
      logger.info("get debug info");
      result = runPythonScript("getDebugInfo");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
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
      logger.info("load csv");
      result = runPythonScript("loadCSV");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 检查Python脚本的输出是否符合预期
    String expected =
        "LoadCSVResp(status=Status(code=200, message=None, subStatus=None), columns=['test.a.a', 'test.a.b', 'test.b.b', 'test.c.c'], recordsNum=4, parseErrorMsg=None)\n"
            + "   key test.a.a test.a.b test.b.b test.c.c\n"
            + "0    0     b'a'     b'b'     None     None\n"
            + "1    1     None     None     b'b'     None\n"
            + "2    2     None     None     None     b'c'\n"
            + "3    3     b'Q'     b'W'     b'E'     b'R'\n"
            + "4    4     b'a'     b'b'      b''      b''\n"
            + "5    5      b''      b''     b'b'      b''\n"
            + "6    6      b''      b''      b''     b'c'\n"
            + "7    7     b'Q'     b'W'     b'E'     b'R'\n";
    assertEquals(expected, result);
  }

  @Test
  public void testLoadDirectory() {
    String result = "";
    try {
      logger.info("load directory");
      result = runPythonScript("loadDirectory");
      logger.info(result);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 检查Python脚本的输出是否符合预期
    List<String> expected = Arrays.asList("   key dir.a dir.b", "0    0  b'1'  b'4'");
    String[] lines = result.split("\n");
    List<String> resultLines = Arrays.asList(lines);
    logger.info(resultLines.toString());
    assertTrue(resultLines.size() >= 2);
    assertEquals(expected, resultLines.subList(resultLines.size() - 2, resultLines.size()));
  }

  @Test
  public void testExport() {
    List<String> result = new ArrayList<>();
    try {
      logger.info("export to file");
      String tmp = runPythonScript("exportToFile");
      logger.info(tmp);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // 验证写入stream是否成功
    String streamPathPrefix =
        String.join(File.separator, System.getProperty("user.dir"), "..", "generated");
    List<String> streamFiles = Arrays.asList("test.a.a", "test.a.b", "test.b.b", "test.c.c");
    result.clear();
    for (String streamFile : streamFiles) {
      String streamPath = streamPathPrefix + File.separator + streamFile;
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
    String outputPath =
        String.join(
            File.separator, System.getProperty("user.dir"), "..", "generated", "output.csv");
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
      logger.info("Clear all data after executing pysession tests.");
      String output = runPythonScript("deleteAll");
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    session.closeSession();
  }
}
