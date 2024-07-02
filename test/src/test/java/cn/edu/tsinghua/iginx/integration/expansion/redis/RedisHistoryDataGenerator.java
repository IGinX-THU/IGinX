/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.integration.expansion.redis;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.readOnlyPort;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class RedisHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisHistoryDataGenerator.class);

  private static final String LOCAL_IP = "127.0.0.1";

  public RedisHistoryDataGenerator() {
    Constant.oriPort = 6379;
    Constant.expPort = 6380;
    Constant.readOnlyPort = 6381;
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    Jedis jedis = new Jedis(LOCAL_IP, port);
    valuesList.forEach(
        row -> {
          for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i);
            String fullPath = pathList.get(i);
            jedis.lpush(fullPath, String.valueOf(value));
          }
        });
    jedis.close();
    LOGGER.info("write data to 127.0.0.1:{} success!", port);
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    writeHistoryData(port, pathList, dataTypeList, new ArrayList<>(), valuesList);
  }

  @Override
  public void writeSpecialHistoryData() {
    try (Jedis jedis = new Jedis(LOCAL_IP, readOnlyPort)) {
      jedis.set("redis.key", "redis value");
      jedis.hset("redis.hash", "field0", "value0");
      jedis.hset("redis.hash", "field1", "value1");
      jedis.zadd("redis.zset", 1, "value0");
      jedis.zadd("redis.zset", 2, "value1");
      jedis.sadd("redis.set", "value0");
      jedis.sadd("redis.set", "value1");
    }
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    Jedis jedis = new Jedis(LOCAL_IP, port);
    jedis.flushDB();
    jedis.close();
    LOGGER.info("clear data on 127.0.0.1:{} success!", port);
  }
}
