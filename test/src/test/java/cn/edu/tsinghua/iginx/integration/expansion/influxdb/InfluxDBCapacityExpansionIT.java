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
package cn.edu.tsinghua.iginx.integration.expansion.influxdb;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.influxdb;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.UsersApi;
import com.influxdb.client.domain.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBCapacityExpansionIT.class);

  public InfluxDBCapacityExpansionIT() {
    super(
        influxdb,
        "username=user, password=12345678, token=testToken, organization=testOrg",
        new InfluxDBHistoryDataGenerator());
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
    wrongExtraParams.add("username=user, password=wrongPwd, token=testToken, organization=testOrg");
    updatedParams.put("password", "newPassword\\,\\\\\"\\'");
  }

  // dummy key range cannot be extended yet
  @Override
  protected void queryExtendedKeyDummy() {}

  @Override
  protected void updateParams(int port) {
    // HTTP传输不涉及转义问题，直接传入原始字符串即可
    changeParams(port, "12345678", "newPassword,\\\"'");
  }

  @Override
  protected void restoreParams(int port) {
    changeParams(port, "newPassword,\\\"'", "12345678");
  }

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "influxdb", 30);
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "influxdb", 30);
  }

  private void changeParams(int port, String oldPassword, String newPassword) {
    String hostUrl = "http://localhost:" + port;
    String adminToken = "testOrg"; // 必须是 Operator Token

    try (InfluxDBClient client = InfluxDBClientFactory.create(hostUrl, adminToken.toCharArray())) {
      UsersApi usersApi = client.getUsersApi();

      // 1. 查找用户
      User user =
          usersApi.findUsers().stream()
              .filter(u -> u.getName().equals("user"))
              .findFirst()
              .orElse(null);

      if (user == null) {
        LOGGER.error("User not found");
        return;
      }

      // 2. 执行修改
      usersApi.updateUserPassword(user.getId(), oldPassword, newPassword);

      LOGGER.info("Password updated successfully for user: user");

    } catch (Exception e) {
      throw new RuntimeException("Failed to update password: " + e.getMessage());
    }
  }
}
