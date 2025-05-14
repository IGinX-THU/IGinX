/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.integration.expansion.dameng;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.readOnlyPort;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DamengCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(
          cn.edu.tsinghua.iginx.integration.expansion.dameng.DamengCapacityExpansionIT.class);
  private static final String newPass = "newPassword";
  private static final HashMap<Integer, String> portsToUsername = new HashMap<>();
  private static final HashMap<Integer, String> portsToPassword = new HashMap<>();

  static {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
    portsToUsername.put(oriPort, "SYSDBA");
    portsToUsername.put(expPort, "SYSDBA");
    portsToUsername.put(readOnlyPort, "SYSDBA");
    portsToPassword.put(oriPort, "SYSDBA001");
    portsToPassword.put(expPort, "SYSDBA001");
    portsToPassword.put(readOnlyPort, "SYSDBA001");
  }

  public DamengCapacityExpansionIT() {
    super(
        StorageEngineType.relational,
        "engine=dameng, username=SYSDBA, password=SYSDBA001",
        new DamengHistoryDataGenerator());
    updatedParams.put("password", newPass);
  }

  @Override
  protected void updateParams(int port) {
    changeParams(port, portsToPassword.get(port), newPass);
  }

  @Override
  protected void restoreParams(int port) {
    changeParams(port, newPass, portsToPassword.get(port));
  }

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "dameng");
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "dameng");
  }

  private void changeParams(int port, String oldPw, String newPw) {
    String username = portsToUsername.get(port);
    String jdbcUrl =
        String.format("jdbc:dm://127.0.0.1:%d?user=SYSDBA&password=%s", port, getQuotName(oldPw));
    try {
      Class.forName("dm.jdbc.driver.DmDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try (Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement stmt = connection.createStatement()) {
      String alterStmt =
          String.format(
              "ALTER USER %s IDENTIFIED BY %s", getQuotName(username), getQuotName(newPw));
      LOGGER.info("alter statement in {}: {}", port, alterStmt);
      stmt.execute(alterStmt);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    testFloatData();
  }

  @Override
  protected void testPathOverlappedDataNotOverlapped() throws SessionException {
    // before
    String statement = "select status from mn.wf01.wt01;";
    String expected =
        "ResultSets:\n"
            + "+-----------------+-------------------+\n"
            + "|              key|mn.wf01.wt01.status|\n"
            + "+-----------------+-------------------+\n"
            + "|32690615153702352|           11111111|\n"
            + "|33357770565002400|           22222222|\n"
            + "+-----------------+-------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    String insert =
        "insert into mn.wf01.wt01 (key, status) values (10, 33333333), (100, 44444444);";
    session.executeSql(insert);

    // after
    statement = "select status from mn.wf01.wt01;";
    expected =
        "ResultSets:\n"
            + "+-----------------+-------------------+\n"
            + "|              key|mn.wf01.wt01.status|\n"
            + "+-----------------+-------------------+\n"
            + "|               10|           33333333|\n"
            + "|              100|           44444444|\n"
            + "|32690615153702352|           11111111|\n"
            + "|33357770565002400|           22222222|\n"
            + "+-----------------+-------------------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  /** 测试float类型数据 */
  private void testFloatData() {
    String statement = "select wt02.float from tm.wf05 where wt02.float <= 44.55;";
    List<String> pathList = Constant.READ_ONLY_FLOAT_PATH_LIST;
    List<List<Object>> valuesList = Constant.READ_ONLY_FLOAT_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
    statement = "select wt02.float from tm.wf05 where wt02.float = 44.55;";
    valuesList = Arrays.asList(Arrays.asList(44.55F));
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  //  private void testFloatData() {
  //    String statement = "select wt02.float from tm.wf05 where wt02.float <= 44.55;";
  //    List<List<Object>> valuesList = Constant.READ_ONLY_FLOAT_VALUES_LIST;
  //    SQLTestTools.executeAndCompare(
  //        session, statement, Constant.READ_ONLY_FLOAT_PATH_LIST, valuesList);
  //    statement = "select wt02.float from tm.wf05 where wt02.float = 44.55;";
  //    valuesList = Arrays.asList(Arrays.asList(44.55F));
  //    SQLTestTools.executeAndCompare(
  //        session, statement, Constant.READ_ONLY_FLOAT_PATH_LIST, valuesList);
  //  }

  //  /** 达梦的用户名不受大小写敏感影响，无论如何都是大写，User对应database，需要修改测试，将databaseName修改为大写 * */
  //  /** 测试float类型数据 */
  //  private void testFloatData() {
  //    String statement = "select wt02.float from TM.wf05 where wt02.float <= 44.55;";
  //    List<String> pathList = splitAndUpperCase(Constant.READ_ONLY_FLOAT_PATH_LIST);
  //    List<List<Object>> valuesList = Constant.READ_ONLY_FLOAT_VALUES_LIST;
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //    statement = "select wt02.float from TM.wf05 where wt02.float = 44.55;";
  //    valuesList = Arrays.asList(Arrays.asList(44.55F));
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //  }
  //
  //  @Override
  //  /** 测试引擎修改参数（目前仅支持dummy & read-only） */
  //  protected void testUpdateEngineParams() throws SessionException {
  //    // 修改前后通过相同schema_prefix查询判断引擎成功更新
  //    LOGGER.info("Testing updating engine params...");
  //    if (updatedParams.isEmpty()) {
  //      LOGGER.info("Engine {} skipped this test.", type);
  //      return;
  //    }
  //
  //    String prefix = "prefix";
  //    // 添加只读节点
  //    addStorageEngine(readOnlyPort, true, true, null, prefix,
  // portsToExtraParams.get(readOnlyPort));
  //    // 查询
  //    String statement = "select wt01.status, wt01.temperature from " + prefix + ".TM.wf05;";
  //    List<String> pathList =
  //        splitAndUpperCase(READ_ONLY_PATH_LIST).stream()
  //            .map(s -> prefix + "." + s)
  //            .collect(Collectors.toList());
  //    List<List<Object>> valuesList = READ_ONLY_VALUES_LIST;
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //
  //    // 修改数据库参数
  //    updateParams(readOnlyPort);
  //
  //    // 修改
  //    List<StorageEngineInfo> engineInfoList = session.getClusterInfo().getStorageEngineInfos();
  //    long id = -1;
  //    for (StorageEngineInfo info : engineInfoList) {
  //      if (info.getIp().equals("127.0.0.1")
  //          && info.getPort() == readOnlyPort
  //          && info.getDataPrefix().equals("null")
  //          && info.getSchemaPrefix().equals(prefix)
  //          && info.getType().equals(type)) {
  //        id = info.getId();
  //      }
  //    }
  //    assertTrue(id != -1);
  //
  //    String newParams =
  //        updatedParams.entrySet().stream()
  //            .map(entry -> entry.getKey() + "=" + entry.getValue())
  //            .collect(Collectors.joining(", "));
  //    session.executeSql(String.format(ALTER_ENGINE_STRING, id, newParams));
  //
  //    // 重新查询
  //    statement = "select wt01.status, wt01.temperature from " + prefix + ".TM.wf05;";
  //    pathList =
  //        splitAndUpperCase(READ_ONLY_PATH_LIST).stream()
  //            .map(s -> prefix + "." + s)
  //            .collect(Collectors.toList());
  //    valuesList = READ_ONLY_VALUES_LIST;
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //
  //    // 删除，不影响后续测试
  //    session.removeStorageEngine(
  //        Collections.singletonList(
  //            new RemovedStorageEngineInfo("127.0.0.1", readOnlyPort, prefix, "")));
  //
  //    // 改回数据库参数
  //    restoreParams(readOnlyPort);
  //  }
  //
  //  @Override
  //  protected void queryExtendedKeyDummy() {
  //    // ori
  //    // extended key queryable
  //    // NOTE: in some database(e.g. mongoDB), the key for dummy data is given randomly and cannot
  // be
  //    // controlled. Thus, when extended value can be queried without specifying key filter,
  //    // we still assume that dummy key range is extended.
  //    String statement = "select wf01.wt01.status, wf01.wt01.temperature from MN;";
  //    SQLTestTools.executeAndContainValue(
  //        session, statement, splitAndUpperCase(ORI_PATH_LIST), ORI_EXTEND_VALUES_LIST);
  //
  //    // exp
  //    statement = "select wf03.wt01.status2 from NT;";
  //    SQLTestTools.executeAndContainValue(
  //        session, statement, splitAndUpperCase(EXP_PATH_LIST1), EXP_EXTEND_VALUES_LIST1);
  //    statement = "select wf04.wt01.temperature from NT;";
  //    SQLTestTools.executeAndContainValue(
  //        session, statement, splitAndUpperCase(EXP_PATH_LIST2), EXP_EXTEND_VALUES_LIST2);
  //
  //    // ro
  //    statement = "select wf05.wt01.status, wf05.wt01.temperature from TM;";
  //    SQLTestTools.executeAndContainValue(
  //        session, statement, splitAndUpperCase(READ_ONLY_PATH_LIST),
  // READ_ONLY_EXTEND_VALUES_LIST);
  //  }
  //
  //  @Override
  //  protected void queryExtendedColDummy() {
  //    // ori
  //    // extended columns unreachable
  //    String statement = "select * from A.a.a;";
  //    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());
  //
  //    // exp
  //    statement = "select * from A.a.b;";
  //    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());
  //
  //    // ro
  //    statement = "select * from A.a.c;";
  //    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());
  //  }
  //
  //  @Override
  //  protected void testPathOverlappedDataNotOverlapped() throws SessionException {
  //    // before
  //    String statement = "select status from MN.wf01.wt01;";
  //    String expected =
  //        "ResultSets:\n"
  //            + "+-----------------+-------------------+\n"
  //            + "|              key|MN.wf01.wt01.status|\n"
  //            + "+-----------------+-------------------+\n"
  //            + "|32690615153702352|           11111111|\n"
  //            + "|33357770565002400|           22222222|\n"
  //            + "+-----------------+-------------------+\n"
  //            + "Total line number = 2\n";
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //
  //    String insert =
  //        "insert into MN.wf01.wt01 (key, status) values (10, 33333333), (100, 44444444);";
  //    session.executeSql(insert);
  //
  //    // after
  //    statement = "select status from MN.wf01.wt01;";
  //    expected =
  //        "ResultSets:\n"
  //            + "+-----------------+-------------------+\n"
  //            + "|              key|MN.wf01.wt01.status|\n"
  //            + "+-----------------+-------------------+\n"
  //            + "|               10|           33333333|\n"
  //            + "|              100|           44444444|\n"
  //            + "|32690615153702352|           11111111|\n"
  //            + "|33357770565002400|           22222222|\n"
  //            + "+-----------------+-------------------+\n"
  //            + "Total line number = 4\n";
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //  }
  //
  //  @Override
  //  protected void testShowColumnsInExpansion(boolean before) {
  //    String statement = "SHOW COLUMNS NT.wf03.*;";
  //    String expected =
  //        "Columns:\n"
  //            + "+--------------------+--------+\n"
  //            + "|                Path|DataType|\n"
  //            + "+--------------------+--------+\n"
  //            + "|NT.wf03.wt01.status2|    LONG|\n"
  //            + "+--------------------+--------+\n"
  //            + "Total line number = 1\n";
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //
  //    statement = "SHOW COLUMNS;";
  //    if (before) {
  //      expected =
  //          "Columns:\n"
  //              + "+------------------------+--------+\n"
  //              + "|                    Path|DataType|\n"
  //              + "+------------------------+--------+\n"
  //              + "|          LN.wf02.status| BOOLEAN|\n"
  //              + "|         LN.wf02.version|  BINARY|\n"
  //              + "|    NT.wf03.wt01.status2|    LONG|\n"
  //              + "|NT.wf04.wt01.temperature|  DOUBLE|\n"
  //              + "+------------------------+--------+\n"
  //              + "Total line number = 4\n";
  //    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
  //      expected =
  //          "Columns:\n"
  //              + "+------------------------+--------+\n"
  //              + "|                    Path|DataType|\n"
  //              + "+------------------------+--------+\n"
  //              + "|          LN.wf02.status| BOOLEAN|\n"
  //              + "|         LN.wf02.version|  BINARY|\n"
  //              + "|    NT.wf03.wt01.status2|    LONG|\n"
  //              + "|NT.wf04.wt01.temperature|  DOUBLE|\n"
  //              + "| p1.NT.wf03.wt01.status2|    LONG|\n"
  //              + "+------------------------+--------+\n"
  //              + "Total line number = 5\n";
  //    }
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //
  //    statement = "SHOW COLUMNS p1.*;";
  //    if (before) {
  //      expected =
  //          "Columns:\n"
  //              + "+----+--------+\n"
  //              + "|Path|DataType|\n"
  //              + "+----+--------+\n"
  //              + "+----+--------+\n"
  //              + "Empty set.\n";
  //    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
  //      expected =
  //          "Columns:\n"
  //              + "+-----------------------+--------+\n"
  //              + "|                   Path|DataType|\n"
  //              + "+-----------------------+--------+\n"
  //              + "|p1.NT.wf03.wt01.status2|    LONG|\n"
  //              + "+-----------------------+--------+\n"
  //              + "Total line number = 1\n";
  //    }
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //
  //    statement = "SHOW COLUMNS *.wf03.wt01.*;";
  //    if (before) {
  //      expected =
  //          "Columns:\n"
  //              + "+--------------------+--------+\n"
  //              + "|                Path|DataType|\n"
  //              + "+--------------------+--------+\n"
  //              + "|NT.wf03.wt01.status2|    LONG|\n"
  //              + "+--------------------+--------+\n"
  //              + "Total line number = 1\n";
  //    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
  //      expected =
  //          "Columns:\n"
  //              + "+-----------------------+--------+\n"
  //              + "|                   Path|DataType|\n"
  //              + "+-----------------------+--------+\n"
  //              + "|   NT.wf03.wt01.status2|    LONG|\n"
  //              + "|p1.NT.wf03.wt01.status2|    LONG|\n"
  //              + "+-----------------------+--------+\n"
  //              + "Total line number = 2\n";
  //    }
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //  }
  //
  //  @Override
  //  protected void testQueryHistoryDataOriHasData() {
  //    String statement = "select wf01.wt01.status, wf01.wt01.temperature from MN;";
  //    List<String> pathList = splitAndUpperCase(ORI_PATH_LIST);
  //    List<List<Object>> valuesList = ORI_VALUES_LIST;
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //  }
  //
  //  @Override
  //  protected void testQueryHistoryDataExpHasData() {
  //    String statement = "select wt01.status2 from NT.wf03;";
  //    List<String> pathList = splitAndUpperCase(EXP_PATH_LIST1);
  //    List<List<Object>> valuesList = EXP_VALUES_LIST1;
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //
  //    statement = "select wt01.temperature from NT.wf04;";
  //    pathList = splitAndUpperCase(EXP_PATH_LIST2);
  //    valuesList = EXP_VALUES_LIST2;
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //  }
  //
  //  @Override
  //  protected void testQueryHistoryDataOriNoData() {
  //    String statement = "select wf01.wt01.status, wf01.wt01.temperature from MN;";
  //    String expect =
  //        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
  //    SQLTestTools.executeAndCompare(session, statement, expect);
  //  }
  //
  //  @Override
  //  protected void testQueryHistoryDataExpNoData() {
  //    String statement = "select wf03.wt01.status, wf04.wt01.temperature from NT;";
  //    String expect =
  //        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
  //    SQLTestTools.executeAndCompare(session, statement, expect);
  //  }
  //
  //  @Override
  //  protected void testQueryHistoryDataReadOnly() {
  //    String statement = "select wt01.status, wt01.temperature from TM.wf05;";
  //    List<String> pathList = splitAndUpperCase(READ_ONLY_PATH_LIST);
  //    List<List<Object>> valuesList = READ_ONLY_VALUES_LIST;
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //  }
  //
  //  @Override
  //  protected void testWriteAndQueryNewData() {
  //    try {
  //      session.executeSql("insert into LN.wf02 (key, status, version) values (100, true,
  // \"v1\");");
  //      session.executeSql("insert into LN.wf02 (key, status, version) values (400, false,
  // \"v4\");");
  //      session.executeSql("insert into LN.wf02 (key, version) values (800, \"v8\");");
  //      queryNewData();
  //    } catch (SessionException e) {
  //      LOGGER.error("insert new data error: ", e);
  //    }
  //  }
  //
  //  @Override
  //  protected void queryNewData() {
  //    String statement = "select * from LN;";
  //    String expect =
  //        "ResultSets:\n"
  //            + "+---+--------------+---------------+\n"
  //            + "|key|LN.wf02.status|LN.wf02.version|\n"
  //            + "+---+--------------+---------------+\n"
  //            + "|100|          true|             v1|\n"
  //            + "|400|         false|             v4|\n"
  //            + "|800|          null|             v8|\n"
  //            + "+---+--------------+---------------+\n"
  //            + "Total line number = 3\n";
  //    SQLTestTools.executeAndCompare(session, statement, expect);
  //
  //    statement = "select count(*) from LN.wf02;";
  //    expect =
  //        "ResultSets:\n"
  //            + "+---------------------+----------------------+\n"
  //            + "|count(LN.wf02.status)|count(LN.wf02.version)|\n"
  //            + "+---------------------+----------------------+\n"
  //            + "|                    2|                     3|\n"
  //            + "+---------------------+----------------------+\n"
  //            + "Total line number = 1\n";
  //    SQLTestTools.executeAndCompare(session, statement, expect);
  //  }
  //
  //  @Override
  //  protected void testWriteAndQueryNewDataAfterCE() {
  //    try {
  //      session.executeSql("insert into LN.wf02 (key, version) values (1600, \"v48\");");
  //      queryAllNewData();
  //    } catch (SessionException e) {
  //      LOGGER.error("insert new data after capacity expansion error: ", e);
  //    }
  //  }
  //
  //  @Override
  //  protected void queryAllNewData() {
  //    String statement = "select * from LN;";
  //    String expect =
  //        "ResultSets:\n"
  //            + "+----+--------------+---------------+\n"
  //            + "| key|LN.wf02.status|LN.wf02.version|\n"
  //            + "+----+--------------+---------------+\n"
  //            + "| 100|          true|             v1|\n"
  //            + "| 400|         false|             v4|\n"
  //            + "| 800|          null|             v8|\n"
  //            + "|1600|          null|            v48|\n"
  //            + "+----+--------------+---------------+\n"
  //            + "Total line number = 4\n";
  //    SQLTestTools.executeAndCompare(session, statement, expect);
  //
  //    statement = "select count(*) from LN.wf02;";
  //    expect =
  //        "ResultSets:\n"
  //            + "+---------------------+----------------------+\n"
  //            + "|count(LN.wf02.status)|count(LN.wf02.version)|\n"
  //            + "+---------------------+----------------------+\n"
  //            + "|                    2|                     4|\n"
  //            + "+---------------------+----------------------+\n"
  //            + "Total line number = 1\n";
  //    SQLTestTools.executeAndCompare(session, statement, expect);
  //  }
  //
  //  @Override
  //  protected void testAddAndRemoveStorageEngineWithPrefix() {
  //    String dataPrefix1 = "NT.wf03";
  //    String dataPrefix2 = "NT.wf04";
  //    String schemaPrefixSuffix = "";
  //    String schemaPrefix1 = "p1";
  //    String schemaPrefix2 = "p2";
  //    String schemaPrefix3 = "p3";
  //
  //    List<List<Object>> valuesList = EXP_VALUES_LIST1;
  //
  //    // 添加不同 schemaPrefix，相同 dataPrefix
  //    testShowColumnsInExpansion(true);
  //    addStorageEngine(
  //        expPort, true, true, dataPrefix1, schemaPrefix1, portsToExtraParams.get(expPort));
  //    testShowColumnsInExpansion(false);
  //
  //    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
  //    String statement = "select status2 from *;";
  //    List<String> pathList = Arrays.asList("NT.wf03.wt01.status2", "p1.NT.wf03.wt01.status2");
  //    SQLTestTools.executeAndCompare(session, statement, pathList, REPEAT_EXP_VALUES_LIST1);
  //
  //    addStorageEngine(
  //        expPort, true, true, dataPrefix1, schemaPrefix2, portsToExtraParams.get(expPort));
  //    testShowClusterInfo(4);
  //
  //    // 如果是重复添加，则报错
  //    String res =
  //        addStorageEngine(
  //            expPort,
  //            true,
  //            true,
  //            dataPrefix1,
  //            schemaPrefix2,
  //            portsToExtraParams.get(expPort),
  //            false);
  //    if (res != null && !res.contains("repeatedly add storage engine")) {
  //      fail();
  //    }
  //    testShowClusterInfo(4);
  //
  //    // data_prefix存在包含关系
  //    res =
  //        addStorageEngine(
  //            expPort, true, true, dataPrefix1, null, portsToExtraParams.get(expPort), false);
  //    if (res != null && !res.contains("duplicate data coverage detected")) {
  //      fail();
  //    }
  //    testShowClusterInfo(4);
  //
  //    addStorageEngine(
  //        expPort, true, true, dataPrefix1, schemaPrefix3, portsToExtraParams.get(expPort));
  //    // 这里是之后待测试的点，如果添加包含关系的，应当报错。
  //    //    res = addStorageEngine(expPort, true, true, "NT.wf03.wt01", "p3");
  //    // 添加相同 schemaPrefix，不同 dataPrefix
  //    addStorageEngine(
  //        expPort, true, true, dataPrefix2, schemaPrefix3, portsToExtraParams.get(expPort));
  //    testShowClusterInfo(6);
  //
  //    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
  //    statement = "select wt01.status2 from p1.NT.wf03;";
  //    pathList = Collections.singletonList("p1.NT.wf03.wt01.status2");
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //
  //    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p2 后查询
  //    statement = "select wt01.status2 from p2.NT.wf03;";
  //    pathList = Collections.singletonList("p2.NT.wf03.wt01.status2");
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //
  //    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = null 后查询
  //    statement = "select wt01.status2 from NT.wf03;";
  //    pathList = Collections.singletonList("NT.wf03.wt01.status2");
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //
  //    // 添加节点 dataPrefix = null && schemaPrefix = p3 后查询
  //    statement = "select wt01.status2 from p3.NT.wf03;";
  //    pathList = Collections.singletonList("p3.NT.wf03.wt01.status2");
  //    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  //
  //    // 通过 session 接口测试移除节点
  //    testShowColumnsRemoveStorageEngine(true);
  //    List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
  //    removedStorageEngineList.add(
  //        new RemovedStorageEngineInfo("127.0.0.1", expPort, "p2" + schemaPrefixSuffix,
  // dataPrefix1));
  //    removedStorageEngineList.add(
  //        new RemovedStorageEngineInfo("127.0.0.1", expPort, "p3" + schemaPrefixSuffix,
  // dataPrefix1));
  //    try {
  //      session.removeStorageEngine(removedStorageEngineList);
  //      testShowClusterInfo(4);
  //    } catch (SessionException e) {
  //      LOGGER.error("remove history data source through session api error: ", e);
  //    }
  //    testShowColumnsRemoveStorageEngine(false);
  //
  //    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p2 + schemaPrefixSuffix 后再查询
  //    statement = "select * from p2.NT.wf03;";
  //    String expect =
  //        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
  //    SQLTestTools.executeAndCompare(session, statement, expect);
  //    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p3 + schemaPrefixSuffix
  //    // 后再查询，测试重点是移除相同 schemaPrefix，不同 dataPrefix
  //    statement = "select wt01.temperature from p3.NT.wf04;";
  //    List<String> pathListAns = new ArrayList<>();
  //    pathListAns.add("p3.NT.wf04.wt01.temperature");
  //    SQLTestTools.executeAndCompare(session, statement, pathListAns, EXP_VALUES_LIST2);
  //
  //    // 通过 sql 语句测试移除节点
  //    String removeStatement = "remove storageengine (\"127.0.0.1\", %d, \"%s\", \"%s\");";
  //    try {
  //      session.executeSql(
  //          String.format(removeStatement, expPort, "p1" + schemaPrefixSuffix, dataPrefix1));
  //      session.executeSql(
  //          String.format(removeStatement, expPort, "p3" + schemaPrefixSuffix, dataPrefix2));
  //      testShowClusterInfo(2);
  //    } catch (SessionException e) {
  //      LOGGER.error("remove history data source through sql error: ", e);
  //    }
  //    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 + schemaPrefixSuffix 后再查询
  //    statement = "select * from p1.NT.wf03;";
  //    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
  //    SQLTestTools.executeAndCompare(session, statement, expect);
  //
  //    try {
  //      session.executeSql(
  //          String.format(removeStatement, expPort, "p1" + schemaPrefixSuffix, dataPrefix1));
  //    } catch (SessionException e) {
  //      if (!e.getMessage().contains("remove history data source failed")) {
  //        LOGGER.error(
  //            "remove history data source should throw error when removing the node that does not
  // exist");
  //        fail();
  //      }
  //    }
  //    testShowClusterInfo(2);
  //  }
  //
  //  @Override
  //  protected void testShowColumnsRemoveStorageEngine(boolean before) {
  //    String statement = "SHOW COLUMNS p1.*, p2.*, p3.*;";
  //    String expected;
  //    if (before) {
  //      expected =
  //          "Columns:\n"
  //              + "+---------------------------+--------+\n"
  //              + "|                       Path|DataType|\n"
  //              + "+---------------------------+--------+\n"
  //              + "|    p1.NT.wf03.wt01.status2|    LONG|\n"
  //              + "|    p2.NT.wf03.wt01.status2|    LONG|\n"
  //              + "|    p3.NT.wf03.wt01.status2|    LONG|\n"
  //              + "|p3.NT.wf04.wt01.temperature|  DOUBLE|\n"
  //              + "+---------------------------+--------+\n"
  //              + "Total line number = 4\n";
  //    } else { // schemaPrefix为p2及p3，dataPrefix为nt.wf03的数据源被移除
  //      expected =
  //          "Columns:\n"
  //              + "+---------------------------+--------+\n"
  //              + "|                       Path|DataType|\n"
  //              + "+---------------------------+--------+\n"
  //              + "|    p1.NT.wf03.wt01.status2|    LONG|\n"
  //              + "|p3.NT.wf04.wt01.temperature|  DOUBLE|\n"
  //              + "+---------------------------+--------+\n"
  //              + "Total line number = 2\n";
  //    }
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //  }
  //
  //  private void testShowClusterInfo(int expected) {
  //    try {
  //      ClusterInfo clusterInfo = session.getClusterInfo();
  //      assertEquals(expected, clusterInfo.getStorageEngineInfos().size());
  //    } catch (SessionException e) {
  //      LOGGER.error("encounter error when showing cluster info: ", e);
  //    }
  //  }
  //
  //  // test dummy and non-dummy columns, in read only test
  //  @Override
  //  public void testShowColumns() {
  //    String statement = "SHOW COLUMNS MN.*;";
  //    String expected =
  //        "Columns:\n"
  //            + "+------------------------+--------+\n"
  //            + "|                    Path|DataType|\n"
  //            + "+------------------------+--------+\n"
  //            + "|     MN.wf01.wt01.status|    LONG|\n"
  //            + "|MN.wf01.wt01.temperature|  DOUBLE|\n"
  //            + "+------------------------+--------+\n"
  //            + "Total line number = 2\n";
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //
  //    statement = "SHOW COLUMNS NT.*;";
  //    expected =
  //        "Columns:\n"
  //            + "+------------------------+--------+\n"
  //            + "|                    Path|DataType|\n"
  //            + "+------------------------+--------+\n"
  //            + "|    NT.wf03.wt01.status2|    LONG|\n"
  //            + "|NT.wf04.wt01.temperature|  DOUBLE|\n"
  //            + "+------------------------+--------+\n"
  //            + "Total line number = 2\n";
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //
  //    statement = "SHOW COLUMNS TM.wf05.wt01.*;";
  //    expected =
  //        "Columns:\n"
  //            + "+------------------------+--------+\n"
  //            + "|                    Path|DataType|\n"
  //            + "+------------------------+--------+\n"
  //            + "|     TM.wf05.wt01.status|    LONG|\n"
  //            + "|TM.wf05.wt01.temperature|  DOUBLE|\n"
  //            + "+------------------------+--------+\n"
  //            + "Total line number = 2\n";
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //  }
  //
  //  @Override
  //  // test dummy query for data out of initial key range (should be visible)
  //  protected void testDummyKeyRange() {
  //    String statement;
  //    statement = "select * from MN.wf01.wt01 where key < 1;";
  //    String expected =
  //        "Columns:\n"
  //            + "+------------------------+--------+\n"
  //            + "|                    Path|DataType|\n"
  //            + "+------------------------+--------+\n"
  //            + "|     MN.wf01.wt01.status|  BINARY|\n"
  //            + "|MN.wf01.wt01.temperature|  BINARY|\n"
  //            + "+------------------------+--------+\n"
  //            + "Total line number = 2\n";
  //    SQLTestTools.executeAndCompare(session, statement, expected);
  //  }
  //
  //  @Override
  //  protected void testSameKeyWarning() {
  //    if (!SUPPORT_KEY.get(testConf.getStorageType())) {
  //      return;
  //    }
  //
  //    try {
  //      session.executeSql(
  //          "insert into MN.wf01.wt01 (key, status) values (0, 123),(1, 123),(2, 123),(3, 123);");
  //      String statement = "select * from MN.wf01.wt01;";
  //
  //      QueryDataSet res = session.executeQuery(statement);
  //      if (res.getWarningMsg() == null
  //          || res.getWarningMsg().isEmpty()
  //          || !res.getWarningMsg().contains("The query results contain overlapped keys.")) {
  //        LOGGER.error("未抛出重叠key的警告");
  //        fail();
  //      }
  //
  //      clearData();
  //
  //      res = session.executeQuery(statement);
  //      if (res.getWarningMsg() != null) {
  //        LOGGER.error("不应抛出重叠key的警告");
  //        fail();
  //      }
  //    } catch (SessionException e) {
  //      LOGGER.error("query data error: ", e);
  //    }
  //  }
  //
  //  private List<String> splitAndUpperCase(List<String> pathList) {
  //    List<String> result = new ArrayList<>();
  //    for (String path : pathList) {
  //      int dotIndex = path.indexOf('.');
  //
  //      String beforeDot = path.substring(0, dotIndex);
  //      String afterDot = path.substring(dotIndex);
  //
  //      // 首字母大写 + 后续保持原样
  //      String capitalized = beforeDot.toUpperCase() + afterDot;
  //      result.add(capitalized);
  //    }
  //    return result;
  //  }

  private static String getQuotName(String name) {
    return "\"" + name + "\"";
  }
}
