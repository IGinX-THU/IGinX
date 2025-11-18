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
package cn.edu.tsinghua.iginx.integration.expansion.mysql;

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

public class MySQLCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLCapacityExpansionIT.class);

  public MySQLCapacityExpansionIT() {
    super(
        StorageEngineType.relational,
        "engine=mysql, username=root",
        new MySQLHistoryDataGenerator());
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
    updatedParams.put("password", "newPassword\\,\\\\\"\\'");
  }

  @Override
  protected void updateParams(int port) {
    changeParams(port, null, updatedParams.get("password"));
  }

  @Override
  protected void restoreParams(int port) {
    // JDBC的密码连接不涉及转义问题，直接传入原始密码即可
    changeParams(port, "newPassword,\\\"'", null);
  }

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "mysql", 30);
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "mysql", 30);
  }

  private void changeParams(int port, String oldPw, String newPw) {
    String jdbcUrl = String.format("jdbc:mysql://127.0.0.1:%d/", port);
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try (Connection connection = DriverManager.getConnection(jdbcUrl, "root", oldPw);
        Statement stmt = connection.createStatement()) {
      String alterStmt = String.format("ALTER USER 'root'@'localhost' IDENTIFIED BY '%s';", newPw);
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
}
