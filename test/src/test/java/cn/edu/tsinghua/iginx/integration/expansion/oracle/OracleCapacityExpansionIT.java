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

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.mysql.MySQLHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools.executeShellScript;
import static org.junit.Assert.fail;

public class OracleCapacityExpansionIT extends BaseCapacityExpansionIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleCapacityExpansionIT.class);

    public OracleCapacityExpansionIT() {
        super(
                StorageEngineType.relational,
                "engine=oracle, username=system",
                new MySQLHistoryDataGenerator());
        ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
        DBConf dbConf = conf.loadDBConf(conf.getStorageType());
        Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
        Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
        Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
        updatedParams.put("password", "newPassword");
    }

    @Override
    protected void updateParams(int port) {
        changeParams(port, null, "newPassword");
    }

    @Override
    protected void restoreParams(int port) {
        changeParams(port, "newPassword", null);
    }

    @Override
    protected void shutdownDatabase(int port) {
        shutOrRestart(port, true, "oralce");
    }

    @Override
    protected void startDatabase(int port) {
        shutOrRestart(port, false, "oralce");
    }

    private void changeParams(int port, String oldPw, String newPw) {
        String scriptPath = updateParamsScriptDir + "oralce.sh";
        String mode = oldPw == null ? "set" : "unset";
        // 脚本参数：对应端口，模式（是在无密码条件下设置密码，还是在有密码条件下去掉密码），需要设置的密码/需要被去掉的密码
        int res =
                executeShellScript(scriptPath, String.valueOf(port), mode, oldPw == null ? newPw : oldPw);
        if (res != 0) {
            fail("Fail to update mysql params.");
        }
    }
    protected void shutOrRestart(int port, boolean mode, String DBName) {
        String dir = mode ? shutdownScriptDir : restartScriptDir;
        String scriptPath = dir + DBName + ".sh";
        int res = executeShellScript(scriptPath, String.valueOf(port));
        if (res != 0) {
            fail("Fail to " + (mode ? "shutdown" : "restart") + " " + DBName + port);
        }
    }
}
