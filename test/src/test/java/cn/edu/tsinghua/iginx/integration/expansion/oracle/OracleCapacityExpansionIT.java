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
package cn.edu.tsinghua.iginx.integration.expansion.oracle;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

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
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleCapacityExpansionIT.class);
  private static final String newPass = "ORCLPWD"; // 新密码保持不变，因为oracle密码错误次数过多会锁定账号
  private static final HashMap<Integer, String> portsToUsername = new HashMap<>();
  private static final HashMap<Integer, String> portsToPassword = new HashMap<>();

  static {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
    portsToUsername.put(oriPort, "SYSTEM");
    portsToUsername.put(expPort, "nt");
    portsToUsername.put(readOnlyPort, "observer");
    portsToPassword.put(oriPort, "ORCLPWD");
    portsToPassword.put(expPort, String.valueOf(expPort));
    portsToPassword.put(readOnlyPort, String.valueOf(readOnlyPort));
  }

  public OracleCapacityExpansionIT() {
    super(
        StorageEngineType.relational,
        new HashMap<Integer, String>() {
          {
            put(
                oriPort,
                "engine=oracle, username="
                    + portsToUsername.get(oriPort)
                    + ", password="
                    + portsToPassword.get(oriPort)
                    + ", database=ORCLPDB");
            put(
                expPort,
                "engine=oracle, username="
                    + portsToUsername.get(expPort)
                    + ", password="
                    + portsToPassword.get(expPort)
                    + ", database=ORCLPDB");
            put(
                readOnlyPort,
                "engine=oracle, username="
                    + portsToUsername.get(readOnlyPort)
                    + ", password="
                    + portsToPassword.get(readOnlyPort)
                    + ", database=ORCLPDB");
          }
        },
        new OracleHistoryDataGenerator());
    updatedParams.put("password", newPass);
  }

  @Override
  protected void updateParams(int port) {
    changeParams(port, newPass);
  }

  @Override
  protected void restoreParams(int port) {
    changeParams(port, portsToPassword.get(port));
  }

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "oracle", 120);
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "oracle", 120);
  }

  private void changeParams(int port, String newPw) {
    String username = portsToUsername.get(port);
    String jdbcUrl =
        String.format("jdbc:oracle:thin:system/ORCLPWD@127.0.0.1:%d/%s", port, "ORCLPDB");
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try (Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement stmt = connection.createStatement()) {
      String alterStmt =
          String.format(
              "ALTER USER %s IDENTIFIED BY %s",
              OracleHistoryDataGenerator.getQuotName(username),
              OracleHistoryDataGenerator.getQuotName(newPw));
      LOGGER.info("alter statement in {}: {}", port, alterStmt);
      stmt.execute(alterStmt);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
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

  @Override
  protected void testShowColumnsInExpansion(boolean before) {
    String statement = "SHOW COLUMNS nt.wf03.*;";
    String expected =
        "Columns:\n"
            + "+--------------------+--------+\n"
            + "|                Path|DataType|\n"
            + "+--------------------+--------+\n"
            + "|nt.wf03.wt01.status2|    LONG|\n"
            + "+--------------------+--------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS;";
    if (before) {
      expected =
          "Columns:\n"
              + "+------------------------+--------+\n"
              + "|                    Path|DataType|\n"
              + "+------------------------+--------+\n"
              + "|          ln.wf02.status| BOOLEAN|\n"
              + "|         ln.wf02.version|  BINARY|\n"
              + "|    nt.wf03.wt01.status2|    LONG|\n"
              + "|nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "+------------------------+--------+\n"
              + "Total line number = 4\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+------------------------+--------+\n"
              + "|                    Path|DataType|\n"
              + "+------------------------+--------+\n"
              + "|          ln.wf02.status| BOOLEAN|\n"
              + "|         ln.wf02.version|  BINARY|\n"
              + "|    nt.wf03.wt01.status2|    LONG|\n"
              + "|nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "| p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+------------------------+--------+\n"
              + "Total line number = 5\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS p1.*;";
    if (before) {
      expected =
          "Columns:\n"
              + "+----+--------+\n"
              + "|Path|DataType|\n"
              + "+----+--------+\n"
              + "+----+--------+\n"
              + "Empty set.\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 1\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS *.wf03.wt01.*;";
    if (before) {
      expected =
          "Columns:\n"
              + "+--------------------+--------+\n"
              + "|                Path|DataType|\n"
              + "+--------------------+--------+\n"
              + "|nt.wf03.wt01.status2|    LONG|\n"
              + "+--------------------+--------+\n"
              + "Total line number = 1\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|   nt.wf03.wt01.status2|    LONG|\n"
              + "|p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);
  }
}
