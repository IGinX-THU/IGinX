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

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.vectordb.tools.CommonUtils;
import cn.edu.tsinghua.iginx.vectordb.tools.MilvusClientPool;
import cn.edu.tsinghua.iginx.vectordb.tools.MilvusClientUtils;
import cn.edu.tsinghua.iginx.vectordb.tools.PathUtils;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.readOnlyPort;
import static cn.edu.tsinghua.iginx.vectordb.tools.Constants.*;
import static cn.edu.tsinghua.iginx.vectordb.tools.MilvusClientPool.createPool;
import static cn.edu.tsinghua.iginx.vectordb.tools.NameUtils.getPathAndVersion;

public class VectorDBHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorDBHistoryDataGenerator.class);

  private static final String LOCAL_IP = "172.18.71.153";

  private static final char SEPARATOR = '.';

  public VectorDBHistoryDataGenerator() {
    Constant.oriPort = 19530;
    Constant.expPort = 19531;
    Constant.readOnlyPort = 19532;
  }


  private void createOrAlterCollections(
          MilvusClientV2 client,
          String databaseName,
          List<String> paths,
          List<Map<String, String>> tagsList,
          List<DataType> dataTypeList)
          throws InterruptedException {
    Map<String, Set<String>> collectionToFields = new HashMap<>();
    Map<String, DataType> fieldToType = new HashMap<>();
    Set<String> collections = new HashSet<>();
    collections.addAll(MilvusClientUtils.listCollections(client, databaseName));
    Map<String, Integer> collectionMap = new HashMap<>();
    collections.forEach(
            collection -> {
              Pair<String, Integer> p = getPathAndVersion(collection);
              collectionMap.put(p.getK(), p.getV());
            });
    for (int i = 0; i < paths.size(); i++) {
      String path = paths.get(i);
      if (!path.startsWith(databaseName+".")){
        continue;
      }
//      path = path.substring(path.indexOf(SEPARATOR)+1);
      Map<String, String> tags = new HashMap<>();
      if (tagsList != null && !tagsList.isEmpty()) {
        tags = tagsList.get(i);
      }
      Pair<String, String> collectionAndField = PathUtils.getCollectionAndFieldByPath(path, tags,true);
      collectionToFields
              .computeIfAbsent(collectionAndField.getK(), k -> new HashSet<>())
              .add(collectionAndField.getV());
      fieldToType.put(collectionAndField.getV(), dataTypeList.get(i));
    }

    for (String collection : collectionToFields.keySet()) {
      if (!collections.contains(collection)) {
        // create collection
        MilvusClientUtils.createCollection(client, databaseName, collection, DataType.LONG);
        //                MilvusClientUtils.createCollection(client, storageUnit, collection,
        // collectionToFields.get(collection), fieldToType,
        //                        DataType.LONG);
      }

      Map<String, String> fields =
              MilvusClientUtils.addCollectionFields(
                      client, databaseName, collection, collectionToFields.get(collection), fieldToType);
    }
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    MilvusClientV2Pool milvusClientV2Pool = null;
    MilvusClientV2 client = null;
    try {
      milvusClientV2Pool =
              MilvusClientPool.createPool("grpc://"+LOCAL_IP+":"+port,"","");
      for (int i = 0; i < 10; i++) {
        try {
          client = milvusClientV2Pool.getClient("default");
          break;
        }catch(Exception e){
          LOGGER.error("Caught a RuntimeException:", e);
          Thread.sleep(1000L);
        }
      }

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
        tablesToColumnIndexes.put(tableName, columnIndexes);
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
          createOrAlterCollections(
                  client, databaseName, pathList, null, dataTypeList);

          List<JsonObject> data = new ArrayList<>();
          long id = 1;
          for (List<Object> values : valuesList) {
            JsonObject row = new JsonObject();
            boolean added = false;
            for (Integer index : item.getValue()) {
              String path = pathList.get(index);
              String columnName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
              added = MilvusClientUtils.addProperty(row, columnName, values.get(index), dataTypeList.get(index));
            }
            if (added) {
              row.addProperty(MILVUS_PRIMARY_FIELD_NAME, id++);
              row.add(
                      MILVUS_VECTOR_FIELD_NAME,
                      new Gson().toJsonTree(CommonUtils.generateFloatVector(DEFAULT_DIMENSION)));
              data.add(row);
            }
          }
          long count =MilvusClientUtils.upsert(client, collectionName, data);
          LOGGER.info("complete insertRows, insertCount:" + count);
        }
      }
      LOGGER.info("write data to "+LOCAL_IP+":{} success!", port);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }finally {
      if (milvusClientV2Pool!=null) {
        if (client != null) {
          milvusClientV2Pool.returnClient("default", client);
        }
        milvusClientV2Pool.clear();
        milvusClientV2Pool.close();
      }
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
    MilvusClientV2Pool milvusClientV2Pool = null;
    MilvusClientV2 client = null;
    try {
      milvusClientV2Pool =
              MilvusClientPool.createPool("grpc://"+LOCAL_IP+":"+port,"","");
      for (int i = 0; i < 10; i++) {
        try {
          client = milvusClientV2Pool.getClient("default");
          break;
        }catch(Exception e){
          LOGGER.error("Caught a RuntimeException:", e);
          try {
            Thread.sleep(1000L);
          } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      List<String> databases = MilvusClientUtils.listDatabase(client);

      for (String database: databases){
        if (!database.equals("default")) {
          MilvusClientUtils.dropDatabase(client, database);
        }
      }
      LOGGER.info("clear data on "+LOCAL_IP+":{} success!", port);
    }finally {
      if (milvusClientV2Pool!=null) {
        if (client != null) {
          milvusClientV2Pool.returnClient("default", client);
        }
        milvusClientV2Pool.clear();
        milvusClientV2Pool.close();
      }
    }
  }
}
