package cn.edu.tsinghua.iginx.integration.expansion.redis;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;

public class RedisHistoryDataGenerator extends BaseHistoryDataGenerator {

    private static final String LOCAL_IP = "127.0.0.1";

    private static final int PORT_A = 6379;

    private static final int PORT_B = 6380;

    @Override
    public void writeHistoryDataToB() throws Exception {
        writeHistoryData(this.seriesA, PORT_A);
    }

    @Override
    public void writeHistoryDataToA() throws Exception {
        writeHistoryData(this.seriesB, PORT_B);
    }

    private void writeHistoryData(
            Map<String, Pair<DataType, List<Pair<Long, Object>>>> series, int port)
            throws Exception {
        Jedis jedis = new Jedis(LOCAL_IP, port);
        series.forEach(
                (path, value) ->
                        value.getV().forEach(pair -> jedis.lpush(path, String.valueOf(pair.v))));
        jedis.close();
    }

    @Override
    public void clearData() {
        Jedis jedisA = new Jedis(LOCAL_IP, PORT_A);
        jedisA.flushDB();
        jedisA.close();

        Jedis jedisB = new Jedis(LOCAL_IP, PORT_B);
        jedisB.flushDB();
        jedisB.close();
    }
}
