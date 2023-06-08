package cn.edu.tsinghua.iginx.integration.expansion.redis;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import redis.clients.jedis.Jedis;

public class RedisHistoryDataGenerator extends BaseHistoryDataGenerator {

    private static final String LOCAL_IP = "127.0.0.1";

    private static final int PORT_A = 6379;

    private static final int PORT_B = 6380;

    @Override
    public void writeHistoryDataToOri() {
        writeHistoryData(PATH_LIST_ORI, VALUES_LIST_ORI, PORT_A);
    }

    @Override
    public void writeHistoryDataToExp() {
        writeHistoryData(PATH_LIST_EXP, VALUES_LIST_EXP, PORT_B);
    }

    private void writeHistoryData(
            List<String> pathList,
            List<List<Object>> valuesList,
            int port) {
        Jedis jedis = new Jedis(LOCAL_IP, port);
        valuesList.forEach(
                row -> {
                    for (int i = 0; i < row.size(); i++) {
                        Object value = row.get(i);
                        jedis.lpush(pathList.get(i), String.valueOf(value));
                    }
                });
        jedis.close();
    }

    @Override
    public void clearHistoryData() {
        Jedis jedisA = new Jedis(LOCAL_IP, PORT_A);
        jedisA.flushDB();
        jedisA.close();

        Jedis jedisB = new Jedis(LOCAL_IP, PORT_B);
        jedisB.flushDB();
        jedisB.close();
    }
}
