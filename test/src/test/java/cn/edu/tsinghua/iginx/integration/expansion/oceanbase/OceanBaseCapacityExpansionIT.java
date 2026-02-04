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
package cn.edu.tsinghua.iginx.integration.expansion.oceanbase;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OceanBaseCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(OceanBaseCapacityExpansionIT.class);

  public OceanBaseCapacityExpansionIT() {
    super(
        StorageEngineType.relational,
        createPortsToExtraParams(
            new HashMap<String, String>() {
              {
                put("engine", "oceanbase");
                put("username", "root@test");
              }
            }),
        new OceanBaseHistoryDataGenerator());
    updatedParams.put("password", "newPassword,\\\"\\'");
  }

  @Override
  protected void updateParams(int port) {
    changeParams(port, null, "newPassword,\\\\\"\\'");
  }

  @Override
  protected void restoreParams(int port) {
    changeParams(port, "newPassword,\\\"'", "");
  }

  private void changeParams(int port, String oldPw, String newPw) {
    String jdbcUrl = String.format("jdbc:oceanbase://127.0.0.1:%d/", port);
    try {
      Class.forName("com.oceanbase.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try (Connection connection = DriverManager.getConnection(jdbcUrl, "root@test", oldPw);
        Statement stmt = connection.createStatement()) {
      String alterStmt = String.format("ALTER USER 'root' IDENTIFIED BY '%s';", newPw);
      LOGGER.info("alter statement in {}: {}", port, alterStmt);
      stmt.execute(alterStmt);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void shutdownDatabase(int port) {}

  @Override
  protected void startDatabase(int port) {}

  @Override
  protected void testDatabaseShutdown() {
    // 内存不足，无法重启
    LOGGER.info("Oceanbase skips test for shutting down data sources.");
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    testFloatData();
  }

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
