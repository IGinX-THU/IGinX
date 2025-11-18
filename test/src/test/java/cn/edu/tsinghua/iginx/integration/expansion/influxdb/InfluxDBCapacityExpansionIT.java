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

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.readOnlyPort;
import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.influxdb;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.OrganizationsApi;
import com.influxdb.client.domain.Organization;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
    wrongExtraParams.add(
        "username=user, password=12345678, token=testToken, organization=wrongOrg");
    updatedParams.put("organization", "newOrg\\,\\\\\"\\'");
  }

  // dummy key range cannot be extended yet
  @Override
  protected void queryExtendedKeyDummy() {}

  @Override
  protected void updateParams(int port) {
    // HTTP传输不涉及转义问题，直接传入原始字符串即可
    changeParams(port, "newOrg,\\\"'");
  }

  @Override
  protected void restoreParams(int port) {
    changeParams(port, "testOrg");
  }

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "influxdb", 30);
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "influxdb", 30);
  }

  @Override
  protected void testAddStorageEngineWithSpecialCharParam(String prefix) throws SessionException {
    String originalParams = portsToExtraParams.get(readOnlyPort);
    String newOrgName = updatedParams.get("organization");

    Map<String, String> paramsMap = new LinkedHashMap<>();
    if (originalParams != null && !originalParams.isEmpty()) {
      for (String pair : originalParams.split(",")) {
        String[] kv = pair.split("=", 2);
        if (kv.length == 2) {
          paramsMap.put(kv[0], kv[1]);
        }
      }
    }
    if (newOrgName != null) {
      paramsMap.put("organization", newOrgName);
    }
    String extraParams =
        paramsMap.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining(","));
    // 添加只读节点
    addStorageEngine(readOnlyPort, true, true, null, prefix, extraParams);
    // 修改
    List<StorageEngineInfo> engineInfoList = session.getClusterInfo().getStorageEngineInfos();
    long id = -1;
    for (StorageEngineInfo info : engineInfoList) {
      if (info.getIp().equals("127.0.0.1")
          && info.getPort() == readOnlyPort
          && info.getDataPrefix().equals("null")
          && info.getSchemaPrefix().equals(prefix)
          && info.getType().equals(type)) {
        id = info.getId();
      }
    }
    assertTrue(id != -1);
    // 删除，不影响后续测试
    session.removeStorageEngine(
        Collections.singletonList(
            new RemovedStorageEngineInfo("127.0.0.1", readOnlyPort, prefix, "")),
        true);
  }

  private void changeParams(int port, String newOrgName) {
    // 1. 定义连接参数
    String hostUrl = "http://localhost:" + port;
    String token = "testToken";

    // 2. 使用 Java API 客户端
    // try-with-resources 确保客户端在使用后自动关闭
    try (InfluxDBClient client = InfluxDBClientFactory.create(hostUrl, token.toCharArray())) {
      OrganizationsApi orgApi = client.getOrganizationsApi();

      // 3. 查找组织
      List<Organization> orgs = orgApi.findOrganizations();
      if (orgs.isEmpty()) {
        fail("Fail to update influxdb params: No organizations found.");
        return;
      }
      // 4. 获取第一个组织并更新它（只有一个组织）
      Organization orgToUpdate = orgs.get(0);
      System.out.println(
          "Updating org ID: " + orgToUpdate.getId() + " from name '" + orgToUpdate.getName() + "'");
      orgToUpdate.setName(newOrgName);
      Organization updatedOrg = orgApi.updateOrganization(orgToUpdate);
      System.out.println("Successfully updated org name to: " + updatedOrg.getName());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Fail to update influxdb params via API: " + e.getMessage());
    }
  }
}
