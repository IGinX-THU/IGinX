package cn.edu.tsinghua.iginx.integration.expansion.redis;

import static cn.edu.tsinghua.iginx.integration.tool.DBType.redis;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCapacityExpansionIT extends BaseCapacityExpansionIT {

private static final Logger logger = LoggerFactory.getLogger(RedisCapacityExpansionIT.class);

public RedisCapacityExpansionIT() {
    super(redis, null, 6379, 6380, 6381);
}
}
