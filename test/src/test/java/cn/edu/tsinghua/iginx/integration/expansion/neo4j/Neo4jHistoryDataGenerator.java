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
package cn.edu.tsinghua.iginx.integration.expansion.neo4j;

import static cn.edu.tsinghua.iginx.neo4j.tools.Constants.IDENTITY_PROPERTY_NAME;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.neo4j.tools.Neo4jClientUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jHistoryDataGenerator.class);

  static final String LOCAL_IP = "127.0.0.1";

  private static final char SEPARATOR = '.';
  private static final String USER = "neo4j";
  private static final String PASSWORD = "neo4jtest";

  private static final String KEY_NAME = "KEYID";

  public Neo4jHistoryDataGenerator() {
    Constant.oriPort = 7687;
    Constant.expPort = 7688;
    Constant.readOnlyPort = 7689;
    LOGGER.info("LOCAL_IP : {}", LOCAL_IP);
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    doWriteHistoryData(port, pathList, dataTypeList, keyList, valuesList);
  }

  public void doWriteHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List keyList,
      List<List<Object>> valuesList) {
    String keyName = IDENTITY_PROPERTY_NAME;
    if (port != Constant.oriPort) {
      keyName = KEY_NAME;
    }

    try (Driver driver = createBoltDriver(port);
        Session session = driver.session()) {
      Map<String, List<Integer>> labelToColumnIndexes = new HashMap<>();
      for (int i = 0; i < pathList.size(); i++) {
        String path = pathList.get(i);
        String labelName = path.substring(0, path.lastIndexOf(SEPARATOR));

        List<Integer> columnIndexes =
            labelToColumnIndexes.computeIfAbsent(labelName, x -> new ArrayList<>());
        columnIndexes.add(i);
      }

      DataType idType = DataType.LONG;
      if (keyList != null
          && keyList.size() > 0
          && !(keyList.get(0) instanceof Long || keyList.get(0) instanceof Integer)) {
        idType = DataType.BINARY;
      }
      for (Map.Entry<String, List<Integer>> item : labelToColumnIndexes.entrySet()) {
        String labelName = item.getKey();
        Neo4jClientUtils.checkAndCreateUniqueConstraint(session, labelName, keyName);
        //        Neo4jClientUtils.getUniqueConstraintName(session, labelName);

        List<Map<String, Object>> data = new ArrayList<>();
        int id = 0;
        for (int i = 0; i < valuesList.size(); i++) {
          List<Object> values = valuesList.get(i);
          Map<String, Object> row = new HashMap<>();
          for (Integer index : item.getValue()) {
            String path = pathList.get(index);
            String propertyName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
            row.put(propertyName, values.get(index));
          }
          if (keyList != null && keyList.size() > i) {
            if (idType == DataType.LONG) {
              row.put(keyName, ((Number) keyList.get(i)).longValue());
            } else {
              row.put(keyName, keyList.get(i));
            }
          } else {
            if (idType == DataType.LONG) {
              row.put(keyName, id++);
            } else {
              row.put(keyName, String.valueOf(id++));
            }
          }

          data.add(row);
        }

        Neo4jClientUtils.bulkInsert(session, labelName, keyName, data);
        LOGGER.info("complete insertRows.");
      }

      LOGGER.info("write data to " + LOCAL_IP + ":{} success!", port);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    writeHistoryData(
        port,
        pathList,
        dataTypeList,
        IntStream.range(0, valuesList.size()).mapToObj(Long::valueOf).collect(Collectors.toList()),
        valuesList);
  }

  @Override
  public void writeSpecialHistoryData() {
    doWriteHistoryData(
        Constant.readOnlyPort,
        Constant.READ_ONLY_FLOAT_PATH_LIST,
        new ArrayList<>(Collections.singletonList(DataType.FLOAT)),
        Arrays.asList("key1", "key2"),
        Constant.READ_ONLY_FLOAT_VALUES_LIST);

    writeHistoryData(
        Constant.readOnlyPort,
        Arrays.asList("d1.c1.i", "d1.c1.b", "d1.c1.f", "d1.c1.s"),
        Arrays.asList(DataType.LONG, DataType.BOOLEAN, DataType.FLOAT, DataType.BINARY),
        Arrays.asList(1L, 2L, 3L, 4L, 5L),
        Arrays.asList(
            Arrays.asList(0L, true, 0.1, "1st"),
            Arrays.asList(1L, false, 1.1, "2nd"),
            Arrays.asList(2L, true, 2.1, "3th"),
            Arrays.asList(3L, false, 3.1, "4th"),
            Arrays.asList(4L, true, 4.1, "5th")));
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    try (Driver driver = createBoltDriver(port);
        Session session = driver.session()) {
      Neo4jClientUtils.clearDatabase(session);
      LOGGER.info("clear data on " + LOCAL_IP + ":{} success!", port);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private Driver createBoltDriver(int port) {
    return createBoltDriver(LOCAL_IP, port, USER, PASSWORD);
  }

  private Driver createBoltDriver(String ip, int port, String user, String password) {
    return GraphDatabase.driver(
        "bolt://" + ip + ":" + port,
        AuthTokens.basic(user, password),
        Config.builder()
            .withMaxConnectionPoolSize(5)
            .withConnectionTimeout(10000, TimeUnit.MILLISECONDS)
            .withConnectionLivenessCheckTimeout(300, java.util.concurrent.TimeUnit.SECONDS)
            .build());
  }
}
