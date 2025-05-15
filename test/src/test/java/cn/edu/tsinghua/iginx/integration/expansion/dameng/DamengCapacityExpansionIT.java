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

  private static String getQuotName(String name) {
    return String.format("\"%s\"", name);
  }
}
