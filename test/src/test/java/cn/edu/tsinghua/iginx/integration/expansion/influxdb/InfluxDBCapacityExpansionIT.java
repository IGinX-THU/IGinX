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

import static cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools.executeShellScript;
import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.influxdb;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
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
    updatedParams.put("organization", "newOrg");
  }

  // dummy key range cannot be extended yet
  @Override
  protected void queryExtendedKeyDummy() {}

  @Override
  protected void updateParams(int port) {
    changeParams(port, "newOrg");
  }

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "influxdb");
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "influxdb");
  }

  @Override
  protected void restoreParams(int port) {
    changeParams(port, "testOrg");
  }

  private void changeParams(int port, String newOrgName) {
    String scriptPath = updateParamsScriptDir + "influxdb.sh";
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("mac")) {
      scriptPath = updateParamsScriptDir + "influxdb_macos.sh";
    } else if (os.contains("win")) {
      scriptPath = updateParamsScriptDir + "influxdb_windows.sh";
    }
    // 脚本参数：对应端口，新参数
    int res = executeShellScript(scriptPath, String.valueOf(port), newOrgName);
    if (res != 0) {
      fail("Fail to update influxdb params.");
    }
  }
}
