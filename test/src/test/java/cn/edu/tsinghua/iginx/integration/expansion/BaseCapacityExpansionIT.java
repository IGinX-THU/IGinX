package cn.edu.tsinghua.iginx.integration.expansion;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;
import static cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools.executeShellScript;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.filesystem.FileSystemCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.influxdb.InfluxDBCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.parquet.ParquetCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.session.ClusterInfo;
import cn.edu.tsinghua.iginx.session.QueryDataSet;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 原始节点相关的变量命名统一用 ori 扩容节点相关的变量命名统一用 exp */
public abstract class BaseCapacityExpansionIT {

  private static final Logger logger = LoggerFactory.getLogger(BaseCapacityExpansionIT.class);

  protected static Session session;

  protected StorageEngineType type;

  protected String extraParams;

  private final boolean IS_PARQUET_OR_FILE_SYSTEM =
      this instanceof FileSystemCapacityExpansionIT || this instanceof ParquetCapacityExpansionIT;

  private final String EXP_SCHEMA_PREFIX = null;

  private final String READ_ONLY_SCHEMA_PREFIX = null;

  public BaseCapacityExpansionIT(StorageEngineType type, String extraParams) {
    this.type = type;
    this.extraParams = extraParams;
  }

  protected String addStorageEngine(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    try {
      StringBuilder statement = new StringBuilder();
      statement.append("ADD STORAGEENGINE (\"127.0.0.1\", ");
      statement.append(port);
      statement.append(", \"");
      statement.append(type.name());
      statement.append("\", \"");
      statement.append("has_data:");
      statement.append(hasData);
      statement.append(", is_read_only:");
      statement.append(isReadOnly);
      if (this instanceof InfluxDBCapacityExpansionIT) {
        statement.append(", url:http://localhost:");
        statement.append(port);
        statement.append("/");
      }
      if (IS_PARQUET_OR_FILE_SYSTEM) {
        statement.append(", dummy_dir:test/");
        statement.append(PORT_TO_ROOT.get(port));
        statement.append(", dir:test/iginx_");
        statement.append(PORT_TO_ROOT.get(port));
        statement.append(", iginx_port:" + oriPortIginx);
      }
      if (extraParams != null) {
        statement.append(", ");
        statement.append(extraParams);
      }
      if (dataPrefix != null) {
        statement.append(", data_prefix:");
        statement.append(dataPrefix);
      }
      if (schemaPrefix != null) {
        statement.append(", schema_prefix:");
        statement.append(schemaPrefix);
      }
      statement.append("\");");

      logger.info("Execute Statement: \"{}\"", statement);
      session.executeSql(statement.toString());
      return null;
    } catch (ExecutionException | SessionException e) {
      logger.warn(
          "add storage engine {} port {} hasData {} isReadOnly {} dataPrefix {} schemaPrefix {} failure: {}",
          type.name(),
          port,
          hasData,
          isReadOnly,
          dataPrefix,
          schemaPrefix,
          e.getMessage());
      return e.getMessage();
    }
  }

  @BeforeClass
  public static void setUp() {
    try {
      session = new Session("127.0.0.1", 6888, "root", "root");
      session.openSession();
    } catch (SessionException e) {
      logger.error("open session error: {}", e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() {
    try {
      session.closeSession();
    } catch (SessionException e) {
      logger.error("close session error: {}", e.getMessage());
    }
  }

  @After
  public void clearData() {
    Controller.clearData(session);
  }

  private void addStorageEngineInProgress(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix)
      throws InterruptedException {
    if (IS_PARQUET_OR_FILE_SYSTEM) {
      startStorageEngineWithIginx(port, hasData, isReadOnly);
    } else {
      addStorageEngine(port, hasData, isReadOnly, dataPrefix, schemaPrefix);
    }
  }

  @Test
  public void oriHasDataExpHasData()
      throws InterruptedException, SessionException, ExecutionException {
    // 查询原始节点的历史数据，结果不为空
    testQueryHistoryDataOriHasData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngineInProgress(expPort, true, true, null, EXP_SCHEMA_PREFIX);

    // 查询扩容节点的历史数据，结果不为空
    testQueryHistoryDataExpHasData();
    // 再次查询新数据
    queryNewData();
    // 再次写入并查询所有新数据
    testWriteAndQueryNewDataAfterCE();
    // 测试插入相同数据后warning
    testSameKeyWarning();
  }

  @Test
  public void oriHasDataExpNoData() throws InterruptedException {
    // 查询原始节点的历史数据，结果不为空
    testQueryHistoryDataOriHasData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngineInProgress(expPort, false, true, null, EXP_SCHEMA_PREFIX);
    // 查询扩容节点的历史数据，结果为空
    testQueryHistoryDataExpNoData();
    // 再次查询新数据
    queryNewData();
    // 再次写入并查询所有新数据
    testWriteAndQueryNewDataAfterCE();
  }

  @Test
  public void oriNoDataExpHasData() throws InterruptedException {
    // 查询原始节点的历史数据，结果为空
    testQueryHistoryDataOriNoData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngineInProgress(expPort, true, true, null, EXP_SCHEMA_PREFIX);
    // 查询扩容节点的历史数据，结果不为空
    testQueryHistoryDataExpHasData();
    // 再次查询新数据
    queryNewData();
    // 再次写入并查询所有新数据
    testWriteAndQueryNewDataAfterCE();
    // 测试带前缀的添加和移除存储引擎操作
    testAddAndRemoveStorageEngineWithPrefix();
  }

  @Test
  public void oriNoDataExpNoData() throws InterruptedException {
    // 查询原始节点的历史数据，结果为空
    testQueryHistoryDataOriNoData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngineInProgress(expPort, false, true, null, EXP_SCHEMA_PREFIX);
    // 查询扩容节点的历史数据，结果为空
    testQueryHistoryDataExpNoData();
    // 再次查询新数据
    queryNewData();
    // 再次写入并查询所有新数据
    testWriteAndQueryNewDataAfterCE();
  }

  @Test
  public void testReadOnly() throws InterruptedException {
    // 查询原始只读节点的历史数据，结果不为空
    testQueryHistoryDataOriHasData();
    // 扩容只读节点
    addStorageEngineInProgress(readOnlyPort, true, true, null, READ_ONLY_SCHEMA_PREFIX);
    // 查询扩容只读节点的历史数据，结果不为空
    testQueryHistoryDataReadOnly();
    // 扩容可写节点
    addStorageEngineInProgress(expPort, true, false, null, EXP_SCHEMA_PREFIX);
    // 查询扩容可写节点的历史数据，结果不为空
    testQueryHistoryDataExpHasData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 再次写入并查询所有新数据
    testWriteAndQueryNewDataAfterCE();

    testQuerySpecialHistoryData();

    if (this instanceof FileSystemCapacityExpansionIT) {
      // 仅用于扩容文件系统后查询文件
      testQueryForFileSystem();
      // TODO 扩容后show columns测试
      testShowColumnsForFileSystem();
    }
  }

  protected void testQuerySpecialHistoryData() {}

  private void testQueryHistoryDataOriHasData() {
    String statement = "select wf01.wt01.status, wf01.wt01.temperature from mn;";
    List<String> pathList = ORI_PATH_LIST;
    List<List<Object>> valuesList = ORI_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  private void testQueryHistoryDataExpHasData() {
    String statement = "select wt01.status2 from nt.wf03;";
    List<String> pathList = EXP_PATH_LIST1;
    List<List<Object>> valuesList = EXP_VALUES_LIST1;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    statement = "select wt01.temperature from nt.wf04;";
    pathList = EXP_PATH_LIST2;
    valuesList = EXP_VALUES_LIST2;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  private void testQueryHistoryDataOriNoData() {
    String statement = "select wf01.wt01.status, wf01.wt01.temperature from mn;";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryHistoryDataExpNoData() {
    String statement = "select wf03.wt01.status, wf04.wt01.temperature from nt;";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryHistoryDataReadOnly() {
    String statement = "select wt01.status, wt01.temperature from tm.wf05;";
    List<String> pathList = READ_ONLY_PATH_LIST;
    List<List<Object>> valuesList = READ_ONLY_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  private void testWriteAndQueryNewData() {
    try {
      session.executeSql("insert into ln.wf02 (key, status, version) values (100, true, \"v1\");");
      session.executeSql("insert into ln.wf02 (key, status, version) values (400, false, \"v4\");");
      session.executeSql("insert into ln.wf02 (key, version) values (800, \"v8\");");
      queryNewData();
    } catch (ExecutionException | SessionException e) {
      logger.error("insert new data error: {}", e.getMessage());
    }
  }

  private void queryNewData() {
    String statement = "select * from ln;";
    String expect =
        "ResultSets:\n"
            + "+---+--------------+---------------+\n"
            + "|key|ln.wf02.status|ln.wf02.version|\n"
            + "+---+--------------+---------------+\n"
            + "|100|          true|             v1|\n"
            + "|400|         false|             v4|\n"
            + "|800|          null|             v8|\n"
            + "+---+--------------+---------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select count(*) from ln.wf02;";
    expect =
        "ResultSets:\n"
            + "+---------------------+----------------------+\n"
            + "|count(ln.wf02.status)|count(ln.wf02.version)|\n"
            + "+---------------------+----------------------+\n"
            + "|                    2|                     3|\n"
            + "+---------------------+----------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testWriteAndQueryNewDataAfterCE() {
    try {
      session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");
      queryAllNewData();
    } catch (ExecutionException | SessionException e) {
      logger.error("insert new data after capacity expansion error: {}", e.getMessage());
    }
  }

  private void queryAllNewData() {
    String statement = "select * from ln;";
    String expect =
        "ResultSets:\n"
            + "+----+--------------+---------------+\n"
            + "| key|ln.wf02.status|ln.wf02.version|\n"
            + "+----+--------------+---------------+\n"
            + "| 100|          true|             v1|\n"
            + "| 400|         false|             v4|\n"
            + "| 800|          null|             v8|\n"
            + "|1600|          null|            v48|\n"
            + "+----+--------------+---------------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select count(*) from ln.wf02;";
    expect =
        "ResultSets:\n"
            + "+---------------------+----------------------+\n"
            + "|count(ln.wf02.status)|count(ln.wf02.version)|\n"
            + "+---------------------+----------------------+\n"
            + "|                    2|                     4|\n"
            + "+---------------------+----------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testAddAndRemoveStorageEngineWithPrefix() {
    String dataPrefix = "nt.wf03";
    String schemaPrefix = "p1";

    // 通过 session 接口测试移除节点
    List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
    removedStorageEngineList.add(
        new RemovedStorageEngineInfo("127.0.0.1", expPort, "", ""));
    try {
      session.removeHistoryDataSource(removedStorageEngineList);
      testShowClusterInfo(1);
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through session api error: {}", e.getMessage());
    }

    addStorageEngine(expPort, true, true, dataPrefix, schemaPrefix);
    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
    String statement = "select status2 from *;";
    List<String> pathList = Arrays.asList("p1.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, EXP_VALUES_LIST1);

    String res = addStorageEngine(expPort, true, true, dataPrefix, schemaPrefix);
    if (res != null && !res.contains("repeatedly add storage engine")) {
      fail();
    }
    testShowClusterInfo(2);

    // 通过 session 接口测试移除节点
    removedStorageEngineList.add(
        new RemovedStorageEngineInfo("127.0.0.1", expPort, schemaPrefix, dataPrefix));
    try {
      session.removeHistoryDataSource(removedStorageEngineList);
      testShowClusterInfo(1);
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through session api error: {}", e.getMessage());
    }
  }

  private void testShowClusterInfo(int expected) {
    try {
      ClusterInfo clusterInfo = session.getClusterInfo();
      assertEquals(expected, clusterInfo.getStorageEngineInfos().size());
    } catch (ExecutionException | SessionException e) {
      logger.error("encounter error when showing cluster info: {}", e.getMessage());
    }
  }

  private void testQueryForFileSystem() {
    try {
      session.executeSql(
          "ADD STORAGEENGINE (\"127.0.0.1\", 6670, \"filesystem\", \"dummy_dir:test/test/a, has_data:true, is_read_only:true, iginx_port:6888, chunk_size_in_bytes:1048576\");");
      String statement = "select 1\\txt from a.*;";
      String expect =
          "ResultSets:\n"
              + "+---+---------------------------------------------------------------------------+\n"
              + "|key|                                                              a.b.c.d.1\\txt|\n"
              + "+---+---------------------------------------------------------------------------+\n"
              + "|  0|979899100101102103104105106107108109110111112113114115116117118119120121122|\n"
              + "+---+---------------------------------------------------------------------------+\n"
              + "Total line number = 1\n";
      SQLTestTools.executeAndCompare(session, statement, expect);

      statement = "select 2\\txt from a.*;";
      expect =
          "ResultSets:\n"
              + "+---+----------------------------------------------------+\n"
              + "|key|                                           a.e.2\\txt|\n"
              + "+---+----------------------------------------------------+\n"
              + "|  0|6566676869707172737475767778798081828384858687888990|\n"
              + "+---+----------------------------------------------------+\n"
              + "Total line number = 1\n";
      SQLTestTools.executeAndCompare(session, statement, expect);

      statement = "select 3\\txt from a.*;";
      expect =
          "ResultSets:\n"
              + "+---+------------------------------------------+\n"
              + "|key|                               a.f.g.3\\txt|\n"
              + "+---+------------------------------------------+\n"
              + "|  0|012345678910111213141516171819202122232425|\n"
              + "+---+------------------------------------------+\n"
              + "Total line number = 1\n";
      SQLTestTools.executeAndCompare(session, statement, expect);
    } catch (SessionException | ExecutionException e) {
      logger.error("test query for file system failed {}", e.getMessage());
      fail();
    }
  }

  private void testShowColumnsForFileSystem() {
    String statement = "SHOW COLUMNS mn.*;";
    String expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     mn.wf01.wt01.status|  BINARY|\n"
            + "|mn.wf01.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS nt.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|    nt.wf03.wt01.status2|  BINARY|\n"
            + "|nt.wf04.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS tm.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     tm.wf05.wt01.status|  BINARY|\n"
            + "|tm.wf05.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS a.*;";
    expected =
        "Columns:\n"
            + "+-------------+--------+\n"
            + "|         Path|DataType|\n"
            + "+-------------+--------+\n"
            + "|a.b.c.d.1\\txt|  BINARY|\n"
            + "|    a.e.2\\txt|  BINARY|\n"
            + "|  a.f.g.3\\txt|  BINARY|\n"
            + "+-------------+--------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  private void testSameKeyWarning() {
    try {
      session.executeSql(
          "insert into mn.wf01.wt01 (key, status) values (0, 123),(1, 123),(2, 123),(3, 123);");
      String statement = "select * from mn.wf01.wt01";

      QueryDataSet res = session.executeQuery(statement);
      if ((res.getWarningMsg() == null || res.getWarningMsg().isEmpty())
          && !res.getWarningMsg().contains("The query results contain overlapped keys.")
          && SUPPORT_KEY.get(type.name().toLowerCase())) {
        logger.error("未抛出重叠key的警告");
        fail();
      }

      clearData();

      res = session.executeQuery(statement);
      if (res.getWarningMsg() != null && SUPPORT_KEY.get(type.name().toLowerCase())) {
        logger.error("不应抛出重叠key的警告");
        fail();
      }
    } catch (ExecutionException | SessionException e) {
      logger.error("query data error: {}", e.getMessage());
    }
  }

  protected void startStorageEngineWithIginx(int port, boolean hasData, boolean isReadOnly)
      throws InterruptedException {
    String scriptPath, iginxPath = ".github/scripts/iginx/iginx.sh";
    String os = System.getProperty("os.name").toLowerCase();
    boolean isOnMac = false;
    if (os.contains("mac")) {
      isOnMac = true;
      iginxPath = ".github/scripts/iginx/iginx_macos.sh";
    }

    if (this instanceof FileSystemCapacityExpansionIT) {
      if (isOnMac) {
        scriptPath = ".github/scripts/dataSources/filesystem_macos.sh";
      } else {
        scriptPath = ".github/scripts/dataSources/filesystem.sh";
      }
    } else if (this instanceof ParquetCapacityExpansionIT) {
      if (isOnMac) {
        scriptPath = ".github/scripts/dataSources/parquet_macos.sh";
      } else {
        scriptPath = ".github/scripts/dataSources/parquet.sh";
      }
    } else {
      throw new IllegalStateException("just support file system and parquet");
    }

    int iginxPort = PORT_TO_IGINXPORT.get(port);
    int restPort = PORT_TO_RESTPORT.get(port);

    int res =
        executeShellScript(
            scriptPath,
            String.valueOf(port),
            String.valueOf(iginxPort),
            "test/" + PORT_TO_ROOT.get(port),
            "test/iginx_" + PORT_TO_ROOT.get(port),
            String.valueOf(hasData),
            String.valueOf(isReadOnly),
            "core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties");
    if (res != 0) {
      fail("change config file fail");
    }

    res = executeShellScript(iginxPath, String.valueOf(iginxPort), String.valueOf(restPort));
    if (res != 0) {
      fail("start iginx fail");
    }

    Thread.sleep(8000);
  }
}
