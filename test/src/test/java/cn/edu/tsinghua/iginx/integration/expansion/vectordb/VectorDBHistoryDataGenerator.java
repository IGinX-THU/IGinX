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
import cn.edu.tsinghua.iginx.vectordb.support.PathSystem;
import cn.edu.tsinghua.iginx.vectordb.support.impl.MilvusPathSystem;
import cn.edu.tsinghua.iginx.vectordb.tools.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorDBHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorDBHistoryDataGenerator.class);

  private static final String LOCAL_IP = "127.0.0.1";

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
      List<DataType> dataTypeList)
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
        // create collection
        MilvusClientUtils.createCollection(
            client,
            databaseName,
            collection,
            DataType.LONG,
            collectionToFields.get(collection),
            fieldToType.get(collection));
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
          client.createDatabase(CreateDatabaseReq.builder().databaseName(databaseName).build());
          LOGGER.info("create database : {}", databaseName);
        } catch (Exception e) {
          LOGGER.info("database {} exists!", databaseName);
        }

        for (Map.Entry<String, List<Integer>> item : entry.getValue().entrySet()) {
          String collectionName = item.getKey();
          createOrAlterCollections(client, databaseName, pathList, null, dataTypeList);
          int id = 1;
          List<JsonObject> data = new ArrayList<>();
          for (int i=0;i<valuesList.size();i++){
            List<Object> values = valuesList.get(i);
            JsonObject row = new JsonObject();
            boolean added = false;
            int pos = 0;
            for (Integer index : item.getValue()) {
              String path = pathList.get(index);
              String columnName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
              added =
                  MilvusClientUtils.addProperty(
                      row, columnName, values.get(pos), dataTypeList.get(pos));
              pos++;
            }
            if (added) {
              if (keyList!=null && keyList.size()>i) {
                row.addProperty(MILVUS_PRIMARY_FIELD_NAME, keyList.get(i));
              }else{
                row.addProperty(MILVUS_PRIMARY_FIELD_NAME, id++);
              }
              row.add(
                  MILVUS_VECTOR_FIELD_NAME,
                  new Gson().toJsonTree(CommonUtils.generateFloatVector(DEFAULT_DIMENSION)));
              data.add(row);
            }
          }

          client.useDatabase(databaseName);
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
        Constant.READ_ONLY_FLOAT_VALUES_LIST);
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
