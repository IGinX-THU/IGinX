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
package cn.edu.tsinghua.iginx.integration.expansion.vectordb;

import static cn.edu.tsinghua.iginx.integration.expansion.vectordb.VectorDBHistoryDataGenerator.LOCAL_IP;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorDBCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorDBCapacityExpansionIT.class);

  public VectorDBCapacityExpansionIT() {
    super(StorageEngineType.vectordb, "", new VectorDBHistoryDataGenerator());
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
  }

  @Override
  protected void updateParams(int port) {}

  @Override
  protected void restoreParams(int port) {}

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "milvus", 120);
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "milvus", 120);
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    testFloatData();
    testFilter();
    testDynamicAndFilter();
    try {
      testShowDummyColumns();
    } catch (SessionException e) {
      e.printStackTrace();
    }
  }

  /** 测试float类型数据 */
  private void testFloatData() {
    String statement = "select wt02.float from tm.wf05 where wt02.float <= 44.55;";
    List<String> pathList = Constant.READ_ONLY_FLOAT_PATH_LIST;
    List<List<Object>> valuesList = Constant.READ_ONLY_FLOAT_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    pathList = Arrays.asList("tm.wf05.wt02.float", "tm.wf05.wt02.vector");
    statement = "select wt02.float,wt02.vector from tm.wf05 where wt02.float = 44.55;";
    valuesList = Arrays.asList(Arrays.asList(44.55F, "[3.0, 4.0]"));
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
    statement = "select * from tm.wf05.wt02;";
    valuesList =
        Arrays.asList(Arrays.asList(22.33F, "[1.0, 2.0]"), Arrays.asList(44.55F, "[3.0, 4.0]"));
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  private void testFilter() {
    String statement = "select i from d1.c1 where i < 3;";
    String expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d1.c1.i|\n"
            + "+---+-------+\n"
            + "|  1|      0|\n"
            + "|  2|      1|\n"
            + "|  3|      2|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select i from d1.c1 where key > 0 and key <= 2;";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d1.c1.i|\n"
            + "+---+-------+\n"
            + "|  1|      0|\n"
            + "|  2|      1|\n"
            + "+---+-------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select b from d1.c1 where b = true;";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d1.c1.b|\n"
            + "+---+-------+\n"
            + "|  1|   true|\n"
            + "|  3|   true|\n"
            + "|  5|   true|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select f from d1.c1 where f > 2;";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d1.c1.f|\n"
            + "+---+-------+\n"
            + "|  3|    2.1|\n"
            + "|  4|    3.1|\n"
            + "|  5|    4.1|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select f from d1.c1 where f > 1.0 and f < 2.9;";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d1.c1.f|\n"
            + "+---+-------+\n"
            + "|  2|    1.1|\n"
            + "|  3|    2.1|\n"
            + "+---+-------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select s from d1.c1 where s = '1st';";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d1.c1.s|\n"
            + "+---+-------+\n"
            + "|  1|    1st|\n"
            + "+---+-------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select s from d1.c1 where s LIKE '.*th';";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d1.c1.s|\n"
            + "+---+-------+\n"
            + "|  3|    3th|\n"
            + "|  4|    4th|\n"
            + "|  5|    5th|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select vector, s from d1.c1 where s = '2nd';";
    expect =
        "ResultSets:\n"
            + "+---+------------+-------+\n"
            + "|key|d1.c1.vector|d1.c1.s|\n"
            + "+---+------------+-------+\n"
            + "|  2|  [1.1, 2.2]|    2nd|\n"
            + "+---+------------+-------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select vector, s from d1.c1 where (s = '2nd' or s LIKE '.*th') and s != '4th';";
    expect =
        "ResultSets:\n"
            + "+---+------------+-------+\n"
            + "|key|d1.c1.vector|d1.c1.s|\n"
            + "+---+------------+-------+\n"
            + "|  2|  [1.1, 2.2]|    2nd|\n"
            + "|  3|  [2.2, 3.3]|    3th|\n"
            + "|  5|  [4.4, 5.5]|    5th|\n"
            + "+---+------------+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select * from d1.c1 where b = false and f > 2 and i < 5;";
    expect =
        "ResultSets:\n"
            + "+---+-------+-------+-------+-------+------------+\n"
            + "|key|d1.c1.b|d1.c1.f|d1.c1.i|d1.c1.s|d1.c1.vector|\n"
            + "+---+-------+-------+-------+-------+------------+\n"
            + "|  4|  false|    3.1|      3|    4th|  [3.3, 4.4]|\n"
            + "+---+-------+-------+-------+-------+------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testDynamicAndFilter() {
    String statement = "select i from d2.c1 where i < 3;";
    String expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d2.c1.i|\n"
            + "+---+-------+\n"
            + "|  1|      0|\n"
            + "|  2|      1|\n"
            + "|  3|      2|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select i from d2.c1 where key > 0 and key <= 2;";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d2.c1.i|\n"
            + "+---+-------+\n"
            + "|  1|      0|\n"
            + "|  2|      1|\n"
            + "+---+-------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select b from d2.c1 where b = true;";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d2.c1.b|\n"
            + "+---+-------+\n"
            + "|  1|   true|\n"
            + "|  3|   true|\n"
            + "|  5|   true|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select f from d2.c1 where f > 2;";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d2.c1.f|\n"
            + "+---+-------+\n"
            + "|  3|    2.1|\n"
            + "|  4|    3.1|\n"
            + "|  5|    4.1|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select f from d2.c1 where f > 1.0 and f < 2.9;";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d2.c1.f|\n"
            + "+---+-------+\n"
            + "|  2|    1.1|\n"
            + "|  3|    2.1|\n"
            + "+---+-------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select s from d2.c1 where s = '1st';";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d2.c1.s|\n"
            + "+---+-------+\n"
            + "|  1|    1st|\n"
            + "+---+-------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select s from d2.c1 where s LIKE '.*th';";
    expect =
        "ResultSets:\n"
            + "+---+-------+\n"
            + "|key|d2.c1.s|\n"
            + "+---+-------+\n"
            + "|  3|    3th|\n"
            + "|  4|    4th|\n"
            + "|  5|    5th|\n"
            + "+---+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select vector, s from d2.c1 where s = '2nd';";
    expect =
        "ResultSets:\n"
            + "+---+------------+-------+\n"
            + "|key|d2.c1.vector|d2.c1.s|\n"
            + "+---+------------+-------+\n"
            + "|  2|  [1.1, 2.2]|    2nd|\n"
            + "+---+------------+-------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select vector, s from d2.c1 where vector='[2.2, 3.3]' limit 1;";
    expect =
        "ResultSets:\n"
            + "+---+------------+-------+\n"
            + "|key|d2.c1.vector|d2.c1.s|\n"
            + "+---+------------+-------+\n"
            + "|  3|  [2.2, 3.3]|    3th|\n"
            + "+---+------------+-------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select vector, s from d2.c1 where (s = '2nd' or s LIKE '.*th') and s != '4th';";
    expect =
        "ResultSets:\n"
            + "+---+------------+-------+\n"
            + "|key|d2.c1.vector|d2.c1.s|\n"
            + "+---+------------+-------+\n"
            + "|  2|  [1.1, 2.2]|    2nd|\n"
            + "|  3|  [2.2, 3.3]|    3th|\n"
            + "|  5|  [4.4, 5.5]|    5th|\n"
            + "+---+------------+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testShowDummyColumns() throws SessionException {
    String schemaPrefix = "dummy";
    int readOnlyPort = 19532;
    Map<String, String> extraParams = new HashMap<>();
    extraParams.put("has_data", "true");
    extraParams.put("is_read_only", "true");
    extraParams.put("schema_prefix", schemaPrefix);
    try {
      session.addStorageEngine(LOCAL_IP, readOnlyPort, StorageEngineType.vectordb, extraParams);
    } catch (SessionException e) {
    }
    String statement = "SHOW COLUMNS dummy.*;";
    String expected =
        "Columns:\n"
            + "+------------------------------+--------+\n"
            + "|                          Path|DataType|\n"
            + "+------------------------------+--------+\n"
            + "|                 dummy.d1.c1.b| BOOLEAN|\n"
            + "|                 dummy.d1.c1.f|   FLOAT|\n"
            + "|                 dummy.d1.c1.i|    LONG|\n"
            + "|                 dummy.d1.c1.s|  BINARY|\n"
            + "|            dummy.d1.c1.vector|  BINARY|\n"
            + "|                 dummy.d2.c1.b| BOOLEAN|\n"
            + "|                 dummy.d2.c1.f|  DOUBLE|\n"
            + "|                 dummy.d2.c1.i|    LONG|\n"
            + "|                 dummy.d2.c1.s|  BINARY|\n"
            + "|            dummy.d2.c1.vector|  BINARY|\n"
            + "|     dummy.tm.wf05.wt01.status|    LONG|\n"
            + "|dummy.tm.wf05.wt01.temperature|  DOUBLE|\n"
            + "|      dummy.tm.wf05.wt02.float|   FLOAT|\n"
            + "|     dummy.tm.wf05.wt02.vector|  BINARY|\n"
            + "+------------------------------+--------+\n"
            + "Total line number = 14\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);
    session.removeStorageEngine(
        Arrays.asList(new RemovedStorageEngineInfo(LOCAL_IP, readOnlyPort, schemaPrefix, "")),
        true);
  }
}
