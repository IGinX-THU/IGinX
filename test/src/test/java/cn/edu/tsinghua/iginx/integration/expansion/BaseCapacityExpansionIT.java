package cn.edu.tsinghua.iginx.integration.expansion;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.influxdb.InfluxDBCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.DBType;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
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

  protected DBType dbType;

  protected String extraParams;

  protected int oriPort;

  protected int expPort;

  protected int readOnlyPort;

  public BaseCapacityExpansionIT(
      DBType dbType, String extraParams, int oriPort, int expPort, int readOnlyPort) {
    this.dbType = dbType;
    this.extraParams = extraParams;
    this.oriPort = oriPort;
    this.expPort = expPort;
    this.readOnlyPort = readOnlyPort;
  }

  protected void addStorageEngine(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    try {
      StringBuilder statement = new StringBuilder();
      statement.append("ADD STORAGEENGINE (\"127.0.0.1\", ");
      statement.append(port);
      statement.append(", \"");
      statement.append(dbType.name());
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
      if (extraParams != null) {
        statement.append(", ");
        statement.append(extraParams);
      }
      if (dataPrefix != null ) {
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
    } catch (ExecutionException | SessionException e) {
      logger.error(
          "add storage engine {} port {} hasData {} isReadOnly {} dataPrefix {} schemaPrefix {} failure: {}",
          dbType.name(),
          port,
          hasData,
          isReadOnly,
          dataPrefix,
          schemaPrefix,
          e.getMessage());
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
  }

  @Test
  public void testQueryHistoryDataOriHasData() {
    String statement = "select * from mn";
    List<String> pathList = BaseHistoryDataGenerator.ORI_PATH_LIST;
    List<List<Object>> valuesList = BaseHistoryDataGenerator.ORI_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    statement = "select count(*) from mn.wf01";
    String expect =
        "ResultSets:\n"
            + "+--------------------------+-------------------------------+\n"
            + "|count(mn.wf01.wt01.status)|count(mn.wf01.wt01.temperature)|\n"
            + "+--------------------------+-------------------------------+\n"
            + "|                         2|                              2|\n"
            + "+--------------------------+-------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryHistoryDataExpHasData() {
    String statement = "select * from mn.wf03";
    List<String> pathList = BaseHistoryDataGenerator.EXP_PATH_LIST;
    List<List<Object>> valuesList = BaseHistoryDataGenerator.EXP_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  private void testQueryHistoryDataOriNoData() {
    String statement = "select * from mn";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryHistoryDataExpNoData() {
    String statement = "select * from mn.wf03";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryHistoryDataReadOnly() {
    String statement = "select * from mn.wf05";
    List<String> pathList = BaseHistoryDataGenerator.READ_ONLY_PATH_LIST;
    List<List<Object>> valuesList = BaseHistoryDataGenerator.READ_ONLY_VALUES_LIST;
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
    addStorageEngine(expPort, true, true, "mn", "p1");
    addStorageEngine(expPort, true, true, "mn", "p2");
    addStorageEngine(expPort, true, true, "mn",null);
    addStorageEngine(expPort, true, true, null,"p3");

    List<List<Object>> valuesList = BaseHistoryDataGenerator.EXP_VALUES_LIST;

    String statement = "select * from p1.mn";
    List<String> pathList = Arrays.asList("p1.mn.wf03.wt01.status", "p1.mn.wf03.wt01.temperature");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    statement = "select * from p2.mn";
    pathList = Arrays.asList("p2.mn.wf03.wt01.status", "p2.mn.wf03.wt01.temperature");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    statement = "select * from mn";
    pathList = Arrays.asList("mn.wf03.wt01.status", "mn.wf03.wt01.temperature");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    statement = "select * from p3.mn";
    pathList = Arrays.asList("p3.mn.wf03.wt01.status", "p3.mn.wf03.wt01.temperature");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 测试移除节点,通过session接口移除节点
    List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
    removedStorageEngineList.add(new RemovedStorageEngineInfo("127.0.0.1", expPort, "p2", "mn"));
    try {
      session.removeHistoryDataSource(removedStorageEngineList);
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through session api error: {}", e.getMessage());
    }
    statement = "select * from p2.mn";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // 通过sql语句测试移除节点
    try {
      session.executeSql(
          "remove historydataresource (\"127.0.0.1\", " + expPort + ", \"p1\", \"mn\")");
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through sql error: {}", e.getMessage());
    }
    statement = "select * from p1.mn";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    removedStorageEngineList.add(new RemovedStorageEngineInfo("127.0.0.1", expPort, "p3", ""));
    removedStorageEngineList.add(new RemovedStorageEngineInfo("127.0.0.1", expPort, "", "mn"));
    try {
      session.removeHistoryDataSource(removedStorageEngineList);
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through session api error: {}", e.getMessage());
    }
  }
}
