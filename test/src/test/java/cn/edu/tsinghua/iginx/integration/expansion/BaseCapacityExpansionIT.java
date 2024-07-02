package cn.edu.tsinghua.iginx.integration.expansion;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;
import static cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools.executeShellScript;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.filesystem.FileSystemCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.influxdb.InfluxDBCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.parquet.ParquetCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.session.ClusterInfo;
import cn.edu.tsinghua.iginx.session.QueryDataSet;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 原始节点相关的变量命名统一用 ori 扩容节点相关的变量命名统一用 exp */
public abstract class BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseCapacityExpansionIT.class);

  protected static Session session;

  private static final ConfLoader testConf = new ConfLoader(Controller.CONFIG_FILE);

  protected StorageEngineType type;

  protected String extraParams;

  protected List<String> wrongExtraParams = new ArrayList<>();

  private final boolean IS_PARQUET_OR_FILE_SYSTEM =
      this instanceof FileSystemCapacityExpansionIT || this instanceof ParquetCapacityExpansionIT;

  private final String EXP_SCHEMA_PREFIX = null;

  private final String READ_ONLY_SCHEMA_PREFIX = null;

  public static final String DBCE_PARQUET_FS_TEST_DIR = "test";

  protected static BaseHistoryDataGenerator generator;

  public BaseCapacityExpansionIT(
      StorageEngineType type, String extraParams, BaseHistoryDataGenerator generator) {
    this.type = type;
    this.extraParams = extraParams;
    BaseCapacityExpansionIT.generator = generator;
  }

  protected String addStorageEngine(
      int port,
      boolean hasData,
      boolean isReadOnly,
      String dataPrefix,
      String schemaPrefix,
      String extraParams) {
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
        statement.append(String.format(", dummy_dir:%s/", DBCE_PARQUET_FS_TEST_DIR));
        statement.append(PORT_TO_ROOT.get(port));
        statement.append(String.format(", dir:%s/iginx_", DBCE_PARQUET_FS_TEST_DIR));
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

      LOGGER.info("Execute Statement: \"{}\"", statement);
      session.executeSql(statement.toString());
      return null;
    } catch (SessionException e) {
      LOGGER.warn(
          "add storage engine:{} port:{} hasData:{} isReadOnly:{} dataPrefix:{} schemaPrefix:{} extraParams:{} failure: ",
          type.name(),
          port,
          hasData,
          isReadOnly,
          dataPrefix,
          schemaPrefix,
          extraParams,
          e);
      return e.getMessage();
    }
  }

  @BeforeClass
  public static void setUp() {
    try {
      session = new Session("127.0.0.1", 6888, "root", "root");
      session.openSession();
    } catch (SessionException e) {
      LOGGER.error("open session error: ", e);
    }
  }

  @AfterClass
  public static void tearDown() {
    try {
      session.closeSession();
    } catch (SessionException e) {
      LOGGER.error("close session error: ", e);
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
      // 测试会添加初始数据，所以hasData=true
      addStorageEngine(port, hasData, isReadOnly, dataPrefix, schemaPrefix, extraParams);
    }
  }

  @Test
  public void oriHasDataExpHasData() throws InterruptedException, SessionException {
    // 查询原始节点的历史数据，结果不为空
    testQueryHistoryDataOriHasData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngineInProgress(expPort, true, false, null, EXP_SCHEMA_PREFIX);

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
    addStorageEngineInProgress(expPort, false, false, null, EXP_SCHEMA_PREFIX);
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
    addStorageEngineInProgress(expPort, true, false, null, EXP_SCHEMA_PREFIX);
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
    addStorageEngineInProgress(expPort, false, false, null, EXP_SCHEMA_PREFIX);
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
    // 测试参数错误的只读节点扩容
    testInvalidDummyParams(readOnlyPort, true, false, null, EXP_SCHEMA_PREFIX);
    // 扩容只读节点
    addStorageEngineInProgress(readOnlyPort, true, true, null, READ_ONLY_SCHEMA_PREFIX);
    // 查询扩容只读节点的历史数据，结果不为空
    testQueryHistoryDataReadOnly();
    // 测试参数错误的可写节点扩容
    testInvalidDummyParams(expPort, true, false, null, EXP_SCHEMA_PREFIX);
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
    }

    // 扩容后show columns测试
    testShowColumns();

    // clear data first, because history generator cannot append. It can only write
    clearData();
    generator.clearHistoryData();

    // 向三个dummy数据库中追加dummy数据，数据的key和列名都在添加数据库时的范围之外
    generator.writeExtendDummyData();
    // 能查到key在初始范围外的数据，查不到列名在初始范围外的数据
    queryExtendedKeyDummy();
    queryExtendedColDummy();
  }

  protected void testInvalidDummyParams(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    // wrong params
    String res;
    for (String params : wrongExtraParams) {
      res = addStorageEngine(port, hasData, isReadOnly, dataPrefix, schemaPrefix, params);
      if (res != null) {
        LOGGER.info(
            "Successfully rejected dummy engine with wrong params: {}; {}. msg: {}",
            port,
            params,
            res);
      } else {
        LOGGER.error("Dummy engine with wrong params {}; {} shouldn't be added.", port, params);
        fail();
      }
    }

    // wrong port
    res = addStorageEngine(port + 999, hasData, isReadOnly, dataPrefix, schemaPrefix, extraParams);
    if (res != null) {
      LOGGER.info(
          "Successfully rejected dummy engine with wrong port: {}; params: {}. msg: {}",
          port + 999,
          extraParams,
          res);
    } else {
      LOGGER.error(
          "Dummy engine with wrong port {} & params:{} shouldn't be added.",
          port + 999,
          extraParams);
      fail();
    }
  }

  protected void queryExtendedKeyDummy() {
    // ori
    // extended key queryable
    // NOTE: in some database(e.g. mongoDB), the key for dummy data is given randomly and cannot be
    // controlled. Thus, when extended value can be queried without specifying key filter,
    // we still assume that dummy key range is extended.
    String statement = "select wf01.wt01.status, wf01.wt01.temperature from mn;";
    SQLTestTools.executeAndContainValue(session, statement, ORI_PATH_LIST, ORI_EXTEND_VALUES_LIST);

    // exp
    statement = "select wf03.wt01.status2 from nt;";
    SQLTestTools.executeAndContainValue(
        session, statement, EXP_PATH_LIST1, EXP_EXTEND_VALUES_LIST1);
    statement = "select wf04.wt01.temperature from nt;";
    SQLTestTools.executeAndContainValue(
        session, statement, EXP_PATH_LIST2, EXP_EXTEND_VALUES_LIST2);

    // ro
    statement = "select wf05.wt01.status, wf05.wt01.temperature from tm;";
    SQLTestTools.executeAndContainValue(
        session, statement, READ_ONLY_PATH_LIST, READ_ONLY_EXTEND_VALUES_LIST);
  }

  protected void queryExtendedColDummy() {
    // ori
    // extended columns unreachable
    String statement = "select * from a.a.a;";
    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());

    // exp
    statement = "select * from a.a.b;";
    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());

    // ro
    statement = "select * from a.a.c;";
    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());
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
    } catch (SessionException e) {
      LOGGER.error("insert new data error: ", e);
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
    } catch (SessionException e) {
      LOGGER.error("insert new data after capacity expansion error: ", e);
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
    String dataPrefix1 = "nt.wf03";
    String dataPrefix2 = "nt.wf04";
    String schemaPrefixSuffix = "";
    String schemaPrefix1 = "p1";
    String schemaPrefix2 = "p2";
    String schemaPrefix3 = "p3";

    List<List<Object>> valuesList = EXP_VALUES_LIST1;

    // 添加不同 schemaPrefix，相同 dataPrefix
    addStorageEngine(expPort, true, true, dataPrefix1, schemaPrefix1, extraParams);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
    String statement = "select status2 from *;";
    List<String> pathList = Arrays.asList("nt.wf03.wt01.status2", "p1.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, REPEAT_EXP_VALUES_LIST1);

    addStorageEngine(expPort, true, true, dataPrefix1, schemaPrefix2, extraParams);
    addStorageEngine(expPort, true, true, dataPrefix1, null, extraParams);
    testShowClusterInfo(5);

    // 如果是重复添加，则报错
    String res = addStorageEngine(expPort, true, true, dataPrefix1, null, extraParams);
    if (res != null && !res.contains("repeatedly add storage engine")) {
      fail();
    }
    testShowClusterInfo(5);

    addStorageEngine(expPort, true, true, dataPrefix1, schemaPrefix3, extraParams);
    // 这里是之后待测试的点，如果添加包含关系的，应当报错。
    //    res = addStorageEngine(expPort, true, true, "nt.wf03.wt01", "p3");
    // 添加相同 schemaPrefix，不同 dataPrefix
    addStorageEngine(expPort, true, true, dataPrefix2, schemaPrefix3, extraParams);
    testShowClusterInfo(7);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
    statement = "select wt01.status2 from p1.nt.wf03;";
    pathList = Collections.singletonList("p1.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p2 后查询
    statement = "select wt01.status2 from p2.nt.wf03;";
    pathList = Collections.singletonList("p2.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = null 后查询
    statement = "select wt01.status2 from nt.wf03;";
    pathList = Collections.singletonList("nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = null && schemaPrefix = p3 后查询
    statement = "select wt01.status2 from p3.nt.wf03;";
    pathList = Collections.singletonList("p3.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 通过 session 接口测试移除节点
    List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
    removedStorageEngineList.add(
        new RemovedStorageEngineInfo("127.0.0.1", expPort, "p2" + schemaPrefixSuffix, dataPrefix1));
    removedStorageEngineList.add(
        new RemovedStorageEngineInfo("127.0.0.1", expPort, "p3" + schemaPrefixSuffix, dataPrefix1));
    try {
      session.removeHistoryDataSource(removedStorageEngineList);
      testShowClusterInfo(5);
    } catch (SessionException e) {
      LOGGER.error("remove history data source through session api error: ", e);
    }
    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p2 + schemaPrefixSuffix 后再查询
    statement = "select * from p2.nt.wf03;";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p3 + schemaPrefixSuffix
    // 后再查询，测试重点是移除相同 schemaPrefix，不同 dataPrefix
    statement = "select wt01.temperature from p3.nt.wf04;";
    List<String> pathListAns = new ArrayList<>();
    pathListAns.add("p3.nt.wf04.wt01.temperature");
    SQLTestTools.executeAndCompare(session, statement, pathListAns, EXP_VALUES_LIST2);

    // 通过 sql 语句测试移除节点
    String removeStatement = "remove historydatasource (\"127.0.0.1\", %d, \"%s\", \"%s\");";
    try {
      session.executeSql(
          String.format(removeStatement, expPort, "p1" + schemaPrefixSuffix, dataPrefix1));
      session.executeSql(
          String.format(removeStatement, expPort, "p3" + schemaPrefixSuffix, dataPrefix2));
      session.executeSql(String.format(removeStatement, expPort, "", dataPrefix1));
      testShowClusterInfo(2);
    } catch (SessionException e) {
      LOGGER.error("remove history data source through sql error: ", e);
    }
    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 + schemaPrefixSuffix 后再查询
    statement = "select * from p1.nt.wf03;";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    try {
      session.executeSql(
          String.format(removeStatement, expPort, "p1" + schemaPrefixSuffix, dataPrefix1));
    } catch (SessionException e) {
      if (!e.getMessage().contains("remove history data source failed")) {
        LOGGER.error(
            "remove history data source should throw error when removing the node that does not exist");
        fail();
      }
    }
    testShowClusterInfo(2);
  }

  private void testShowClusterInfo(int expected) {
    try {
      ClusterInfo clusterInfo = session.getClusterInfo();
      assertEquals(expected, clusterInfo.getStorageEngineInfos().size());
    } catch (SessionException e) {
      LOGGER.error("encounter error when showing cluster info: ", e);
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
    } catch (SessionException e) {
      LOGGER.error("test query for file system failed ", e);
      fail();
    }
  }

  // test dummy and non-dummy columns, in read only test
  @Test
  public void testShowColumns() {
    String statement = "SHOW COLUMNS mn.*;";
    String expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     mn.wf01.wt01.status|    LONG|\n"
            + "|mn.wf01.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS nt.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|    nt.wf03.wt01.status2|    LONG|\n"
            + "|nt.wf04.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS tm.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     tm.wf05.wt01.status|    LONG|\n"
            + "|tm.wf05.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  // test dummy query for data out of initial key range (should be visible)
  protected void testDummyKeyRange() {
    String statement;
    statement = "select * from mn where key < 1;";
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
  }

  private void testSameKeyWarning() {
    try {
      session.executeSql(
          "insert into mn.wf01.wt01 (key, status) values (0, 123),(1, 123),(2, 123),(3, 123);");
      String statement = "select * from mn.wf01.wt01;";

      QueryDataSet res = session.executeQuery(statement);
      if ((res.getWarningMsg() == null
              || res.getWarningMsg().isEmpty()
              || !res.getWarningMsg().contains("The query results contain overlapped keys."))
          && SUPPORT_KEY.get(testConf.getStorageType())) {
        LOGGER.error("未抛出重叠key的警告");
        fail();
      }

      clearData();

      res = session.executeQuery(statement);
      if (res.getWarningMsg() != null && SUPPORT_KEY.get(testConf.getStorageType())) {
        LOGGER.error("不应抛出重叠key的警告");
        fail();
      }
    } catch (SessionException e) {
      LOGGER.error("query data error: ", e);
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
    } else if (os.contains("win")) {
      iginxPath = ".github/scripts/iginx/iginx_windows.sh";
    }

    if (this instanceof FileSystemCapacityExpansionIT) {
      if (isOnMac) {
        scriptPath = ".github/scripts/dataSources/filesystem_macos.sh";
      } else {
        scriptPath = ".github/scripts/dataSources/filesystem_linux_windows.sh";
      }
    } else if (this instanceof ParquetCapacityExpansionIT) {
      if (isOnMac) {
        scriptPath = ".github/scripts/dataSources/parquet_macos.sh";
      } else {
        scriptPath = ".github/scripts/dataSources/parquet_linux_windows.sh";
      }
    } else {
      throw new IllegalStateException("Only support file system and parquet");
    }

    int iginxPort = PORT_TO_IGINXPORT.get(port);
    int restPort = PORT_TO_RESTPORT.get(port);

    // env only applies in github action currently
    String metadataStorage = System.getenv("METADATA_STORAGE");
    if (metadataStorage == null || !metadataStorage.equalsIgnoreCase("etcd")) {
      metadataStorage = "zookeeper";
    } else {
      metadataStorage = "etcd";
    }

    int res =
        executeShellScript(
            scriptPath,
            String.valueOf(port),
            String.valueOf(iginxPort),
            hasData
                ? DBCE_PARQUET_FS_TEST_DIR + "/" + PORT_TO_ROOT.get(port)
                : DBCE_PARQUET_FS_TEST_DIR + "/" + INIT_PATH_LIST.get(0).replace(".", "/"),
            DBCE_PARQUET_FS_TEST_DIR + "/iginx_" + PORT_TO_ROOT.get(port),
            String.valueOf(hasData),
            String.valueOf(isReadOnly),
            "core/target/iginx-core-*/conf/config.properties",
            metadataStorage);
    if (res != 0) {
      fail("change config file fail");
    }

    res = executeShellScript(iginxPath, String.valueOf(iginxPort), String.valueOf(restPort));
    if (res != 0) {
      fail("start iginx fail");
    }
  }
}
