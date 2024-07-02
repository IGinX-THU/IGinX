package cn.edu.tsinghua.iginx.integration.expansion.mysql;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLCapacityExpansionIT.class);

  public MySQLCapacityExpansionIT() {
    super(
        StorageEngineType.relational,
        "engine:mysql, username:root,"
            + "meta_properties_path:dataSources/relational/src/main/resources/mysql-meta-template.properties",
        new MySQLHistoryDataGenerator());
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
  }
}
