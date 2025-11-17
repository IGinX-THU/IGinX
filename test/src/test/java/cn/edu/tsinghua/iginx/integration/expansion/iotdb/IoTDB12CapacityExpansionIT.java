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
package cn.edu.tsinghua.iginx.integration.expansion.iotdb;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.iotdb12;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB12CapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB12CapacityExpansionIT.class);

  public IoTDB12CapacityExpansionIT() {
    super(
        iotdb12,
        "username=root, password=root, sessionPoolSize=20",
        new IoTDB12HistoryDataGenerator());
    wrongExtraParams.add("username=root, password=wrong, sessionPoolSize=20");
    wrongExtraParams.add("username=wrong, password=root, sessionPoolSize=20");
    updatedParams.put("password", "newPassword");
  }

  @Override
  protected void updateParams(int port) {
    changeParams(port, "root", updatedParams.get("password"));
  }

  @Override
  protected void restoreParams(int port) {
    changeParams(port, updatedParams.get("password"), "root");
  }

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "iotdb", 30);
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "iotdb", 30);
  }

  private void changeParams(int port, String oldPw, String newPw) {
    try {
      Session session = new Session("127.0.0.1", port, "root", oldPw);
      session.open();
      session.executeNonQueryStatement(String.format("ALTER USER root SET PASSWORD '%s';", newPw));
      session.close();
      LOGGER.info("alter password to 127.0.0.1:{} success!", port);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      LOGGER.error("alter password to 127.0.0.1:{} failure: ", port, e);
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
}
