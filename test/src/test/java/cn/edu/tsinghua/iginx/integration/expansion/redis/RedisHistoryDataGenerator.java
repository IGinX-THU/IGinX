package cn.edu.tsinghua.iginx.integration.expansion.redis;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class RedisHistoryDataGenerator extends BaseHistoryDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(RedisHistoryDataGenerator.class);

    private static final String LOCAL_IP = "127.0.0.1";

    public RedisHistoryDataGenerator() {
        this.oriPort = 6379;
        this.expPort = 6380;
        this.readOnlyPort = 6381;
    }

    @Override
    public void writeHistoryData(
            int port,
            List<String> pathList,
            List<DataType> dataTypeList,
            List<List<Object>> valuesList) {
        Jedis jedis = new Jedis(LOCAL_IP, port);
        valuesList.forEach(
                row -> {
                    for (int i = 0; i < row.size(); i++) {
                        Object value = row.get(i);
                        jedis.lpush(pathList.get(i), String.valueOf(value));
                    }
                });
        jedis.close();
        logger.info("write data to 127.0.0.1:{} success!", port);
    }

    @Override
    public void clearHistoryData(int port) {
        Jedis jedis = new Jedis(LOCAL_IP, port);
        jedis.flushDB();
        jedis.close();
        logger.info("clear data on 127.0.0.1:{} success!", port);
    }
}
