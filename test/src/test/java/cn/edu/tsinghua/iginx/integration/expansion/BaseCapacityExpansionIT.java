package cn.edu.tsinghua.iginx.integration.expansion;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.filesystem.FileSystemCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.influxdb.InfluxDBCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.parquet.ParquetCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
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

  private static final Logger logger = LoggerFactory.getLogger(BaseCapacityExpansionIT.class);

  protected static Session session;

  protected StorageEngineType type;

  protected String extraParams;

  private final boolean IS_PARQUET_OR_FILE_SYSTEM =
      this instanceof FileSystemCapacityExpansionIT || this instanceof ParquetCapacityExpansionIT;

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

  @Test
  public void oriHasDataExpHasData() {
    // 查询原始节点的历史数据，结果不为空
    testQueryHistoryDataOriHasData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngine(expPort, true, false, null, null);
    // 查询扩容节点的历史数据，结果不为空
    testQueryHistoryDataExpHasData();
    // 再次查询新数据
    queryNewData();
    // 再次写入并查询所有新数据
    testWriteAndQueryNewDataAfterCE();
  }

  @Test
  public void oriHasDataExpNoData() {
    // 查询原始节点的历史数据，结果不为空
    testQueryHistoryDataOriHasData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngine(expPort, false, false, null, null);
    // 查询扩容节点的历史数据，结果为空
    testQueryHistoryDataExpNoData();
    // 再次查询新数据
    queryNewData();
    // 再次写入并查询所有新数据
    testWriteAndQueryNewDataAfterCE();
  }

  @Test
  public void oriNoDataExpHasData() {
    // 查询原始节点的历史数据，结果为空
    testQueryHistoryDataOriNoData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngine(expPort, true, false, null, null);
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
  public void oriNoDataExpNoData() {
    // 查询原始节点的历史数据，结果为空
    testQueryHistoryDataOriNoData();
    // 写入并查询新数据
    testWriteAndQueryNewData();
    // 扩容
    addStorageEngine(expPort, false, false, null, null);
    // 查询扩容节点的历史数据，结果为空
    testQueryHistoryDataExpNoData();
    // 再次查询新数据
    queryNewData();
    // 再次写入并查询所有新数据
    testWriteAndQueryNewDataAfterCE();
  }

  @Test
  public void testReadOnly() {
    // 查询原始只读节点的历史数据，结果不为空
    testQueryHistoryDataOriHasData();
    // 扩容只读节点
    addStorageEngine(readOnlyPort, true, true, null, null);
    // 查询扩容只读节点的历史数据，结果不为空
    testQueryHistoryDataReadOnly();
    // 扩容可写节点
    addStorageEngine(expPort, true, false, null, null);
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
    String statement = "select wf01.wt01.status, wf01.wt01.temperature from mn";
    List<String> pathList = ORI_PATH_LIST;
    List<List<Object>> valuesList = ORI_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  private void testQueryHistoryDataExpHasData() {
    String statement = "select wt01.status2 from nt.wf03";
    List<String> pathList = EXP_PATH_LIST1;
    List<List<Object>> valuesList = EXP_VALUES_LIST1;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    statement = "select wt01.temperature from nt.wf04";
    pathList = EXP_PATH_LIST2;
    valuesList = EXP_VALUES_LIST2;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  private void testQueryHistoryDataOriNoData() {
    String statement = "select wf01.wt01.status, wf01.wt01.temperature from mn";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryHistoryDataExpNoData() {
    String statement = "select wf03.wt01.status, wf04.wt01.temperature from nt";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryHistoryDataReadOnly() {
    String statement = "select wt01.status, wt01.temperature from tm.wf05";
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
    String statement = "select * from ln";
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

    statement = "select count(*) from ln.wf02";
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
    String statement = "select * from ln";
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

    statement = "select count(*) from ln.wf02";
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
    String dataPrefix1 = IS_PARQUET_OR_FILE_SYSTEM ? "wf03" : "nt.wf03";
    String dataPrefix2 = IS_PARQUET_OR_FILE_SYSTEM ? "wf04" : "nt.wf04";
    String schemaPrefixSuffix = IS_PARQUET_OR_FILE_SYSTEM ? ".nt" : "";
    String schemaPrefix = IS_PARQUET_OR_FILE_SYSTEM ? "nt" : "";
    List<List<Object>> valuesList = EXP_VALUES_LIST1;

    // 添加不同 schemaPrefix，相同 dataPrefix
    addStorageEngine(expPort, true, true, dataPrefix1, "p1");

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
    String statement = "select status2 from *";
    List<String> pathList = Arrays.asList("nt.wf03.wt01.status2", "p1.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, REPEAT_EXP_VALUES_LIST1);

    addStorageEngine(expPort, true, true, dataPrefix1, "p2");
    addStorageEngine(expPort, true, true, dataPrefix1, null);

    // 如果是重复添加，则报错
    String res = addStorageEngine(expPort, true, true, dataPrefix1, null);
    if (res != null && !res.contains("unexpected repeated add")) {
      fail();
    }
    addStorageEngine(expPort, true, true, dataPrefix1, "p3");
    // 这里是之后待测试的点，如果添加包含关系的，应当报错。
    //    res = addStorageEngine(expPort, true, true, "nt.wf03.wt01", "p3");
    // 添加相同 schemaPrefix，不同 dataPrefix
    addStorageEngine(expPort, true, true, dataPrefix2, "p3");

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
    statement = "select wt01.status2 from p1.nt.wf03";
    pathList = Collections.singletonList("p1.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p2 后查询
    statement = "select wt01.status2 from p2.nt.wf03";
    pathList = Collections.singletonList("p2.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = null 后查询
    statement = "select wt01.status2 from nt.wf03";
    pathList = Collections.singletonList("nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = null && schemaPrefix = p3 后查询
    statement = "select wt01.status2 from p3.nt.wf03";
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
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through session api error: {}", e.getMessage());
    }
    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p2 + schemaPrefixSuffix 后再查询
    statement = "select * from p2.nt.wf03";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p3 + schemaPrefixSuffix
    // 后再查询，测试重点是移除相同 schemaPrefix，不同 dataPrefix
    statement = "select wt01.temperature from p3.nt.wf04";
    List<String> pathListAns = new ArrayList<>();
    pathListAns.add("p3.nt.wf04.wt01.temperature");
    SQLTestTools.executeAndCompare(session, statement, pathListAns, EXP_VALUES_LIST2);

    // 通过 sql 语句测试移除节点
    String removeStatement = "remove historydatasource (\"127.0.0.1\", %d, \"%s\", \"%s\")";
    try {
      session.executeSql(
          String.format(removeStatement, expPort, "p1" + schemaPrefixSuffix, dataPrefix1));
      session.executeSql(
          String.format(removeStatement, expPort, "p3" + schemaPrefixSuffix, dataPrefix2));
      session.executeSql(String.format(removeStatement, expPort, schemaPrefix, dataPrefix1));
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through sql error: {}", e.getMessage());
    }
    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 + schemaPrefixSuffix 后再查询
    statement = "select * from p1.nt.wf03";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    try {
      session.executeSql(
          String.format(removeStatement, expPort, "p1" + schemaPrefixSuffix, dataPrefix1));
    } catch (ExecutionException | SessionException e) {
      if (!e.getMessage().contains("dummy storage engine does not exist.")) {
        logger.error(
            "'remove history data source should throw error when removing the node that does not exist");
        fail();
      }
    }
  }

  private void testQueryForFileSystem() {
    try {
      session.executeSql(
          "ADD STORAGEENGINE (\"127.0.0.1\", 6670, \"filesystem\", \"dummy_dir:test/test/a, has_data:true, is_read_only:true, iginx_port:6888, chunk_size_in_bytes:1048576\")");
      String statement = "select 1\\txt from a.*";
      String expect =
          "ResultSets:\n"
              + "+---+---------------------------------------------------------------------------+\n"
              + "|key|                                                              a.b.c.d.1\\txt|\n"
              + "+---+---------------------------------------------------------------------------+\n"
              + "|  0|979899100101102103104105106107108109110111112113114115116117118119120121122|\n"
              + "+---+---------------------------------------------------------------------------+\n"
              + "Total line number = 1\n";
      SQLTestTools.executeAndCompare(session, statement, expect);

      statement = "select 2\\txt from a.*";
      expect =
          "ResultSets:\n"
              + "+---+----------------------------------------------------+\n"
              + "|key|                                           a.e.2\\txt|\n"
              + "+---+----------------------------------------------------+\n"
              + "|  0|6566676869707172737475767778798081828384858687888990|\n"
              + "+---+----------------------------------------------------+\n"
              + "Total line number = 1\n";
      SQLTestTools.executeAndCompare(session, statement, expect);

      statement = "select 3\\txt from a.*";
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
}
