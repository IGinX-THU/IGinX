package cn.edu.tsinghua.iginx.integration.expansion.redis;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.redis;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisCapacityExpansionIT.class);

  public RedisCapacityExpansionIT() {
    super(redis, null);
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    String statement = "select * from redis;";
    String expect =
        "ResultSets:\n"
            + "+---+--------------+----------------+-----------+---------+----------+\n"
            + "|key|redis.hash.key|redis.hash.value|  redis.key|redis.set|redis.zset|\n"
            + "+---+--------------+----------------+-----------+---------+----------+\n"
            + "|  0|        field1|          value1|redis value|   value1|    value0|\n"
            + "|  1|        field0|          value0|       null|   value0|    value1|\n"
            + "+---+--------------+----------------+-----------+---------+----------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }
}
