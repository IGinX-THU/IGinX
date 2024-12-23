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
package cn.edu.tsinghua.iginx.integration.expansion.vectordb;

import static cn.edu.tsinghua.iginx.vectordb.tools.Constants.*;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.vectordb.tools.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorDBHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorDBHistoryDataGenerator.class);

  static final String LOCAL_IP = "127.0.0.1";

  private static final char SEPARATOR = '.';

  public VectorDBHistoryDataGenerator() {
    Constant.oriPort = 19530;
    Constant.expPort = 19531;
    Constant.readOnlyPort = 19532;
    LOGGER.info("LOCAL_IP : {}", LOCAL_IP);
  }

  private void createOrAlterCollections(
      MilvusClientV2 client,
      String databaseName,
      List<String> paths,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypeList,
      String vectorFieldName,
      DataType idType)
      throws InterruptedException, UnsupportedEncodingException {
    Map<String, Set<String>> collectionToFields = new HashMap<>();
    Map<String, Map<String, DataType>> fieldToType = new HashMap<>();
    Set<String> collections = new HashSet<>();
    collections.addAll(MilvusClientUtils.listCollections(client, databaseName));
    for (int i = 0; i < paths.size(); i++) {
      String path = paths.get(i);
      if (!path.startsWith(databaseName + ".")) {
        continue;
      }
      Map<String, String> tags = new HashMap<>();
      if (tagsList != null && !tagsList.isEmpty()) {
        tags = tagsList.get(i);
      }
      Pair<String, String> collectionAndField =
          PathUtils.getCollectionAndFieldByPath(path, tags, true);
      collectionToFields
          .computeIfAbsent(collectionAndField.getK(), k -> new HashSet<>())
          .add(collectionAndField.getV());
      fieldToType
          .computeIfAbsent(collectionAndField.getK(), s -> new HashMap<>())
          .put(collectionAndField.getV(), dataTypeList.get(i));
    }

    for (String collection : collectionToFields.keySet()) {
      if (!collections.contains(collection)) {
        MilvusClientUtils.createCollection(
            client,
            databaseName,
            collection,
            idType,
            collectionToFields.get(collection),
            fieldToType.get(collection),
            vectorFieldName);
      }
    }
  }

  private void createOrAlterDynamicCollections(
      MilvusClientV2 client,
      String databaseName,
      List<String> paths,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypeList,
      String vectorFieldName,
      DataType idType)
      throws InterruptedException, UnsupportedEncodingException {
    Map<String, Set<String>> collectionToFields = new HashMap<>();
    Map<String, Map<String, DataType>> fieldToType = new HashMap<>();
    Set<String> collections = new HashSet<>();
    collections.addAll(MilvusClientUtils.listCollections(client, databaseName));
    for (int i = 0; i < paths.size(); i++) {
      String path = paths.get(i);
      if (!path.startsWith(databaseName + ".")) {
        continue;
      }
      Map<String, String> tags = new HashMap<>();
      if (tagsList != null && !tagsList.isEmpty()) {
        tags = tagsList.get(i);
      }
      Pair<String, String> collectionAndField =
          PathUtils.getCollectionAndFieldByPath(path, tags, true);
      collectionToFields
          .computeIfAbsent(collectionAndField.getK(), k -> new HashSet<>())
          .add(collectionAndField.getV());
      fieldToType
          .computeIfAbsent(collectionAndField.getK(), s -> new HashMap<>())
          .put(collectionAndField.getV(), dataTypeList.get(i));
    }

    for (String collection : collectionToFields.keySet()) {
      if (!collections.contains(collection)) {
        MilvusClientUtils.createDynamicCollection(
            client,
            databaseName,
            collection,
            idType,
            collectionToFields.get(collection),
            fieldToType.get(collection),
            vectorFieldName);
      }
    }
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    writeHistoryData(port, pathList, dataTypeList, keyList, valuesList, null);
  }

  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List keyList,
      List<List<Object>> valuesList,
      String vectorFieldName) {
    writeHistoryData(port, pathList, dataTypeList, keyList, valuesList, vectorFieldName, null);
  }

  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List keyList,
      List<List<Object>> valuesList,
      String vectorFieldName,
      List<List<Float>> vectorValuesList) {
    if (StringUtils.isEmpty(vectorFieldName)) {
      vectorFieldName = MILVUS_VECTOR_FIELD_NAME;
    }
    try (MilvusClient milvusClient = new MilvusClient("grpc", LOCAL_IP, port, null)) {
      MilvusClientV2 client = milvusClient.getClient();
      Map<String, Map<String, List<Integer>>> databaseToTablesToColumnIndexes = new HashMap<>();
      for (int i = 0; i < pathList.size(); i++) {
        String path = pathList.get(i);
        String databaseName = path.substring(0, path.indexOf(SEPARATOR));
        String tableName = path.substring(path.indexOf(SEPARATOR) + 1, path.lastIndexOf(SEPARATOR));

        Map<String, List<Integer>> tablesToColumnIndexes =
            databaseToTablesToColumnIndexes.computeIfAbsent(databaseName, x -> new HashMap<>());
        List<Integer> columnIndexes =
            tablesToColumnIndexes.computeIfAbsent(tableName, x -> new ArrayList<>());
        columnIndexes.add(i);
      }

      for (Map.Entry<String, Map<String, List<Integer>>> entry :
          databaseToTablesToColumnIndexes.entrySet()) {
        String databaseName = entry.getKey();
        try {
          client.createDatabase(
              CreateDatabaseReq.builder().databaseName(NameUtils.escape(databaseName)).build());
          LOGGER.info("create database : {}", databaseName);
        } catch (Exception e) {
          LOGGER.info("database {} exists!", databaseName);
        }

        DataType idType = DataType.LONG;
        if (keyList != null && keyList.size() > 0 && keyList.get(0) instanceof String) {
          idType = DataType.BINARY;
        }
        for (Map.Entry<String, List<Integer>> item : entry.getValue().entrySet()) {
          String collectionName = item.getKey();
          createOrAlterCollections(
              client, databaseName, pathList, null, dataTypeList, vectorFieldName, idType);
          int id = 0;
          List<JsonObject> data = new ArrayList<>();
          for (int i = 0; i < valuesList.size(); i++) {
            List<Object> values = valuesList.get(i);
            JsonObject row = new JsonObject();
            boolean added = false;
            for (Integer index : item.getValue()) {
              String path = pathList.get(index);
              String columnName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
              added =
                  MilvusClientUtils.addProperty(
                      row, columnName, values.get(index), dataTypeList.get(index));
            }
            if (added) {
              if (keyList != null && keyList.size() > i) {
                if (idType == DataType.LONG) {
                  row.addProperty(MILVUS_PRIMARY_FIELD_NAME, ((Number) keyList.get(i)).longValue());
                } else {
                  row.addProperty(MILVUS_PRIMARY_FIELD_NAME, (String) keyList.get(i));
                }
              } else {
                if (idType == DataType.LONG) {
                  row.addProperty(MILVUS_PRIMARY_FIELD_NAME, id++);
                } else {
                  row.addProperty(MILVUS_PRIMARY_FIELD_NAME, String.valueOf(id++));
                }
              }
              if (vectorValuesList != null && vectorValuesList.size() > i) {
                row.add(vectorFieldName, new Gson().toJsonTree(vectorValuesList.get(i)));
              } else {
                row.add(
                    vectorFieldName,
                    new Gson().toJsonTree(CommonUtils.generateFloatVector(DEFAULT_DIMENSION)));
              }
              data.add(row);
            }
          }

          MilvusClientUtils.useDatabase(client, databaseName);
          long count = MilvusClientUtils.upsert(client, collectionName, data);
          LOGGER.info("complete insertRows, insertCount:" + count);
        }
      }
      LOGGER.info("write data to " + LOCAL_IP + ":{} success!", port);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
  }

  public void writeDynamicData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List keyList,
      List<List<Object>> valuesList,
      String vectorFieldName,
      List<List<Float>> vectorValuesList) {
    if (StringUtils.isEmpty(vectorFieldName)) {
      vectorFieldName = MILVUS_VECTOR_FIELD_NAME;
    }
    try (MilvusClient milvusClient = new MilvusClient("grpc", LOCAL_IP, port, null)) {
      MilvusClientV2 client = milvusClient.getClient();
      Map<String, Map<String, List<Integer>>> databaseToTablesToColumnIndexes = new HashMap<>();
      for (int i = 0; i < pathList.size(); i++) {
        String path = pathList.get(i);
        String databaseName = path.substring(0, path.indexOf(SEPARATOR));
        String tableName = path.substring(path.indexOf(SEPARATOR) + 1, path.lastIndexOf(SEPARATOR));

        Map<String, List<Integer>> tablesToColumnIndexes =
            databaseToTablesToColumnIndexes.computeIfAbsent(databaseName, x -> new HashMap<>());
        List<Integer> columnIndexes =
            tablesToColumnIndexes.computeIfAbsent(tableName, x -> new ArrayList<>());
        columnIndexes.add(i);
      }

      for (Map.Entry<String, Map<String, List<Integer>>> entry :
          databaseToTablesToColumnIndexes.entrySet()) {
        String databaseName = entry.getKey();
        try {
          client.createDatabase(
              CreateDatabaseReq.builder().databaseName(NameUtils.escape(databaseName)).build());
          LOGGER.info("create database : {}", databaseName);
        } catch (Exception e) {
          LOGGER.info("database {} exists!", databaseName);
        }

        DataType idType = DataType.LONG;
        if (keyList != null && keyList.size() > 0 && keyList.get(0) instanceof String) {
          idType = DataType.BINARY;
        }
        for (Map.Entry<String, List<Integer>> item : entry.getValue().entrySet()) {
          String collectionName = item.getKey();
          createOrAlterDynamicCollections(
              client, databaseName, pathList, null, dataTypeList, vectorFieldName, idType);
          int id = 0;
          List<JsonObject> data = new ArrayList<>();
          for (int i = 0; i < valuesList.size(); i++) {
            List<Object> values = valuesList.get(i);
            JsonObject row = new JsonObject();
            boolean added = false;
            for (Integer index : item.getValue()) {
              String path = pathList.get(index);
              String columnName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
              added =
                  MilvusClientUtils.addProperty(
                      row, columnName, values.get(index), dataTypeList.get(index));
            }
            if (added) {
              if (keyList != null && keyList.size() > i) {
                if (idType == DataType.LONG) {
                  row.addProperty(MILVUS_PRIMARY_FIELD_NAME, ((Number) keyList.get(i)).longValue());
                } else {
                  row.addProperty(MILVUS_PRIMARY_FIELD_NAME, (String) keyList.get(i));
                }
              } else {
                if (idType == DataType.LONG) {
                  row.addProperty(MILVUS_PRIMARY_FIELD_NAME, id++);
                } else {
                  row.addProperty(MILVUS_PRIMARY_FIELD_NAME, String.valueOf(id++));
                }
              }
              if (vectorValuesList != null && vectorValuesList.size() > i) {
                row.add(vectorFieldName, new Gson().toJsonTree(vectorValuesList.get(i)));
              } else {
                row.add(
                    vectorFieldName,
                    new Gson().toJsonTree(CommonUtils.generateFloatVector(DEFAULT_DIMENSION)));
              }
              data.add(row);
            }
          }

          MilvusClientUtils.useDatabase(client, databaseName);
          long count = MilvusClientUtils.upsert(client, collectionName, data);
          LOGGER.info("complete insertRows, insertCount:" + count);
        }
      }
      LOGGER.info("write data to " + LOCAL_IP + ":{} success!", port);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    writeHistoryData(port, pathList, dataTypeList, new ArrayList<>(), valuesList);
  }

  @Override
  public void writeSpecialHistoryData() {
    writeHistoryData(
        Constant.readOnlyPort,
        Constant.READ_ONLY_FLOAT_PATH_LIST,
        new ArrayList<>(Collections.singletonList(DataType.FLOAT)),
        Arrays.asList("key1", "key2"),
        Constant.READ_ONLY_FLOAT_VALUES_LIST,
        "vector",
        Arrays.asList(Arrays.asList(1.0f, 2.0f), Arrays.asList(3.0f, 4.0f)));

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
            Arrays.asList(4L, true, 4.1, "5th")),
        "vector",
        Arrays.asList(
            Arrays.asList(0.0f, 1.1f),
            Arrays.asList(1.1f, 2.2f),
            Arrays.asList(2.2f, 3.3f),
            Arrays.asList(3.3f, 4.4f),
            Arrays.asList(4.4f, 5.5f)));

    writeDynamicData(
        Constant.readOnlyPort,
        Arrays.asList("d2.c1.i", "d2.c1.b", "d2.c1.f", "d2.c1.s"),
        Arrays.asList(DataType.LONG, DataType.BOOLEAN, DataType.FLOAT, DataType.BINARY),
        Arrays.asList(1L, 2L, 3L, 4L, 5L),
        Arrays.asList(
            Arrays.asList(0L, true, 0.1, "1st"),
            Arrays.asList(1L, false, 1.1, "2nd"),
            Arrays.asList(2L, true, 2.1, "3th"),
            Arrays.asList(3L, false, 3.1, "4th"),
            Arrays.asList(4L, true, 4.1, "5th")),
        "vector",
        Arrays.asList(
            Arrays.asList(0.0f, 1.1f),
            Arrays.asList(1.1f, 2.2f),
            Arrays.asList(2.2f, 3.3f),
            Arrays.asList(3.3f, 4.4f),
            Arrays.asList(4.4f, 5.5f)));
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    try (MilvusClient milvusClient = new MilvusClient("grpc", LOCAL_IP, port, null)) {
      MilvusClientV2 client = milvusClient.getClient();
      List<String> databases = MilvusClientUtils.listDatabase(client);

      for (String database : databases) {
        if (!database.equals("default")) {
          MilvusClientUtils.dropDatabase(client, database);
        }
      }
      LOGGER.info("clear data on " + LOCAL_IP + ":{} success!", port);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
