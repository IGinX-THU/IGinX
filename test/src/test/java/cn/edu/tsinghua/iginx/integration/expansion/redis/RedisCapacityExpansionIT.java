package cn.edu.tsinghua.iginx.integration.expansion.redis;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.redis;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger logger = LoggerFactory.getLogger(RedisCapacityExpansionIT.class);

  public RedisCapacityExpansionIT() {
    super(redis, null);
    Constant.oriPort = 6379;
    Constant.expPort = 6380;
    Constant.readOnlyPort = 6381;
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
