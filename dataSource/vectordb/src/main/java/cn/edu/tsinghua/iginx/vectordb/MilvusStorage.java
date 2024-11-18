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
package cn.edu.tsinghua.iginx.vectordb;

import static cn.edu.tsinghua.iginx.vectordb.tools.Constants.*;
import static cn.edu.tsinghua.iginx.vectordb.tools.MilvusClientPool.createPool;
import static cn.edu.tsinghua.iginx.vectordb.tools.MilvusClientUtils.*;
import static cn.edu.tsinghua.iginx.vectordb.tools.NameUtils.getPathAndVersion;
import static cn.edu.tsinghua.iginx.vectordb.tools.TagKVUtils.splitFullName;
import static cn.edu.tsinghua.iginx.vectordb.tools.TagKVUtils.toFullName;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import cn.edu.tsinghua.iginx.vectordb.entity.VectorDBQueryRowStream;
import cn.edu.tsinghua.iginx.vectordb.support.PathSystem;
import cn.edu.tsinghua.iginx.vectordb.support.impl.MilvusPathSystem;
import cn.edu.tsinghua.iginx.vectordb.tools.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MilvusStorage implements IStorage {

  /** Milvus 客户端池，用于管理与 Milvus 服务的连接。 */
  private final MilvusClientV2Pool milvusClientV2Pool;

  private final StorageEngineMeta meta;

  private static final Logger LOGGER = LoggerFactory.getLogger(MilvusStorage.class);

  private static final String DEFAULT_KEY = "default";

  public static final int BATCH_SIZE = 10000;

  Map<String, PathSystem> pathSystemMap = new HashMap<>();

  /**
   * 获取存储引擎的连接 URL。
   *
   * @param meta 存储引擎的元数据。
   * @return 生成的连接 URL。
   */
  protected String getUrl(StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    return new StringBuilder(extraParams.getOrDefault(DB_PROTOCOL, DEFAULT_DB_PROTOCOL))
        .append("://")
        .append(meta.getIp())
        .append(":")
        .append(meta.getPort())
        .toString();
  }

  /**
   * 构造函数，用于初始化 MilvusStorage 实例。
   *
   * @param meta 存储引擎的元数据。
   * @throws StorageInitializationException 如果存储引擎类型不匹配或初始化过程中发生错误。
   */
  public MilvusStorage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(StorageEngineType.vectordb)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    this.meta = meta;
    Map<String, String> extraParams = meta.getExtraParams();
    LOGGER.info("init milvus url: " + getUrl(meta));
    this.milvusClientV2Pool =
        createPool(
            getUrl(meta),
            extraParams.get(USERNAME),
            extraParams.get(PASSWORD),
            MilvusClientPool.getPoolConfig(extraParams));
//    MilvusClientV2 client = null;
//    try {
//      for (int i = 0; i < 5; i++) {
//        client = milvusClientV2Pool.getClient(DEFAULT_KEY);
//        if (client != null) {
//          break;
//        }
//        try {
//          Thread.sleep(2000L);
//        } catch (InterruptedException e) {
//        }
//      }
//    } finally {
//      if (client != null) {
//        milvusClientV2Pool.returnClient(DEFAULT_KEY, client);
//      }
//    }

    //    try {
    //      Thread.sleep(10000);
    //
    //      ConnectConfig config =
    // ConnectConfig.builder().uri("grpc://192.168.120.147:19530").build();
    //      MilvusClientV2 client = new MilvusClientV2(config);
    //      PathUtils.getPathSystem(client, pathSystem);
    //    } catch (Exception e) {
    //      e.printStackTrace();
    //    }

  }

  //    private void createCollections(
  //            MilvusClientV2 client,
  //            String storageUnit,
  //            List<String> paths,
  //            List<Map<String, String>> tagsList,
  //            List<DataType> dataTypeList) {
  //        for (int i = 0; i < paths.size(); i++) {
  //            String path = paths.get(i);
  //            Map<String, String> tags = new HashMap<>();
  //            if (tagsList != null && !tagsList.isEmpty()) {
  //                tags = tagsList.get(i);
  //            }
  //            VectorDBSchema schema = new VectorDBSchema(path, QUOTA);
  //            String collectionName = schema.getCollectionName();
  //
  //            List<String> tables = getCollections(storageUnit, collectionName);
  //            if (tables.isEmpty()) {
  //                client.createCollection(CreateCollectionReq.builder()
  //
  // .collectionName(collectionName).primaryFieldName(MILVUS_PRIMARY_FIELD_NAME)
  //                        .vectorFieldName(MILVUS_VECTOR_FIELD_NAME)
  //                        .dimension(DEFAULT_DIMENSION)
  //                        .build());
  //                LOGGER.info("[Create] create collection : {}", collectionName);
  //            }
  //        }
  //    }

  private void createOrAlterCollections(
      MilvusClientV2 client,
      String storageUnit,
      List<String> paths,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypeList)
      throws InterruptedException {
    client.useDatabase(NameUtils.escape(storageUnit));
    Map<String, Set<String>> collectionToFields = new HashMap<>();
    Map<String, DataType> fieldToType = new HashMap<>();
    Set<String> collections = new HashSet<>();
    collections.addAll(MilvusClientUtils.listCollections(client, storageUnit));
    Map<String, Integer> collectionMap = new HashMap<>();
    collections.forEach(
        collection -> {
          Pair<String, Integer> p = getPathAndVersion(collection);
          collectionMap.put(p.getK(), p.getV());
        });
    for (int i = 0; i < paths.size(); i++) {
      String path = paths.get(i);
      Map<String, String> tags = new HashMap<>();
      if (tagsList != null && !tagsList.isEmpty()) {
        tags = tagsList.get(i);
      }
      Pair<String, String> collectionAndField =
          PathUtils.getCollectionAndFieldByPath(path, tags, false);
      collectionToFields
          .computeIfAbsent(collectionAndField.getK(), k -> new HashSet<>())
          .add(collectionAndField.getV());
      fieldToType.put(collectionAndField.getV(), dataTypeList.get(i));
    }

    for (String collection : collectionToFields.keySet()) {
      if (!collections.contains(collection)) {
        // create collection
        MilvusClientUtils.createCollection(client, storageUnit, collection, DataType.LONG);
        //                MilvusClientUtils.createCollection(client, storageUnit, collection,
        // collectionToFields.get(collection), fieldToType,
        //                        DataType.LONG);
      }

      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(storageUnit, s -> new MilvusPathSystem(storageUnit));
      Map<String, String> fields =
          MilvusClientUtils.addCollectionFields(
              client,
              storageUnit,
              collection,
              collectionToFields.get(collection),
              fieldToType,
              pathSystem);
    }
  }

  private List<String> getCollections(String databaseName, String collectionPattern) {
    MilvusClientV2 client = this.getClient(databaseName);
    if (client == null) {
      return new ArrayList<>();
    }
    ListCollectionsResp resp = client.listCollections();
    List<String> collectionNames = resp.getCollectionNames();

    if (collectionPattern == null || collectionPattern.indexOf("*") > 0) {
      return collectionNames;
    }

    List<String> matchedCollectionNames = new ArrayList<>();
    for (String collectionName : collectionNames) {
      if (Pattern.matches(StringUtils.reformatPath(collectionPattern), collectionName)) {
        matchedCollectionNames.add(collectionName);
      }
    }

    return matchedCollectionNames;
  }

  private Exception insertNonAlignedRowRecords(
      MilvusClientV2 client, String databaseName, RowDataView data) {
    int batchSize = Math.min(data.getKeySize(), BATCH_SIZE);
    try {
      //            createCollections(client, databaseName, data.getPaths(), data.getTagsList(),
      // data.getDataTypeList());

      // 插入数据
      Map<String, List<JsonObject>> tableToRowEntries = new HashMap<>(); // <表名, 插入数据>
      int cnt = 0;
      while (cnt < data.getKeySize()) {
        int size = Math.min(data.getKeySize() - cnt, batchSize);
        for (int i = cnt; i < cnt + size; i++) {
          BitmapView bitmapView = data.getBitmapView(i);

          Map<String, JsonObject> tableToRowEntry = new HashMap<>();
          for (int j = 0; j < data.getPathNum(); j++) {
            String path = data.getPath(j);
            DataType dataType = data.getDataType(j);
            VectorDBSchema schema = new VectorDBSchema(path, QUOTA);
            String collectionName = schema.getCollectionName();
            String columnName = schema.getFieldName();
            Map<String, String> tags = data.getTags(j);
            collectionName = toFullName(collectionName, tags);
            JsonObject row;
            if (tableToRowEntry.containsKey(collectionName)) {
              row = tableToRowEntry.get(collectionName);
            } else {
              row = new JsonObject();
              tableToRowEntry.put(collectionName, row);
            }
            boolean empty = true;
            if (bitmapView.get(j)) {
              switch (dataType) {
                case BINARY:
                  row.addProperty(
                      columnName, new String((byte[]) data.getValue(j, i), StandardCharsets.UTF_8));
                  empty = false;
                  break;
                case BOOLEAN:
                case LONG:
                case DOUBLE:
                case INTEGER:
                case FLOAT:
                  row.addProperty(columnName, (Number) data.getValue(j, i));
                  empty = false;
                  break;
                default:
                  break;
              }
            }
            if (empty) {
              tableToRowEntry.remove(collectionName);
            } else {
              row.addProperty(Constants.KEY_NAME, data.getKey(i));
              row.add(
                  Constants.VECTOR_NAME,
                  new Gson().toJsonTree(CommonUtils.generateFloatVector(DEFAULT_DIMENSION)));
            }
          }
          for (Map.Entry<String, JsonObject> entry : tableToRowEntry.entrySet()) {
            if (tableToRowEntries.containsKey(entry.getKey())) {
              tableToRowEntries.get(entry.getKey()).add(entry.getValue());
            } else {
              List<JsonObject> list = new ArrayList<>();
              list.add(entry.getValue());
              tableToRowEntries.put(entry.getKey(), list);
            }
          }
        }

        for (Map.Entry<String, List<JsonObject>> entry : tableToRowEntries.entrySet()) {
          InsertResp resp =
              client.insert(
                  InsertReq.builder()
                      .collectionName(entry.getKey())
                      .data(entry.getValue())
                      .build());
          List<Object> ids = resp.getPrimaryKeys();
          LOGGER.info("complete insertRows, insertCount:" + ids.size());
        }
        cnt += size;
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return e;
    }
    return null;
  }

  private Exception insertRecords(MilvusClientV2 client, String databaseName, DataView data) {
    int batchSize = Math.min(data.getKeySize(), BATCH_SIZE);
    try {
      createOrAlterCollections(
          client, databaseName, data.getPaths(), data.getTagsList(), data.getDataTypeList());

      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(databaseName, s -> new MilvusPathSystem(databaseName));
      // 插入数据
      Map<String, List<JsonObject>> tableToRowEntries = new HashMap<>(); // <表名, 插入数据>
      int cnt = 0;
      Set fields = new HashSet();
      while (cnt < data.getKeySize()) {
        int size = Math.min(data.getKeySize() - cnt, batchSize);
        List ids = new ArrayList<>();
        for (int i = cnt; i < cnt + size; i++) {
          Map<String, JsonObject> tableToRowEntry = new HashMap<>();
          for (int j = 0; j < data.getPathNum(); j++) {
            String path = data.getPath(j);
            DataType dataType = data.getDataType(j);
            Map<String, String> tags = data.getTags(j);
            path = PathUtils.getPathSystem(client, pathSystem).findPath(path, tags);
            String collectionName = path.substring(0, path.lastIndexOf("."));
            String columnName = path.substring(path.lastIndexOf(".") + 1);
            fields.add(columnName);
            JsonObject row = tableToRowEntry.computeIfAbsent(collectionName, k -> new JsonObject());
            Object obj;
            if (data instanceof RowDataView) {
              obj = data.getValue(i, j);
            } else {
              obj = data.getValue(j, i);
            }
            boolean added = MilvusClientUtils.addProperty(row, columnName, obj, dataType);
            if (!added) {
              tableToRowEntry.remove(collectionName);
            } else {
              ids.add(data.getKey(i));
              row.addProperty(MILVUS_PRIMARY_FIELD_NAME, data.getKey(i));
              row.add(
                  MILVUS_VECTOR_FIELD_NAME,
                  new Gson().toJsonTree(CommonUtils.generateFloatVector(DEFAULT_DIMENSION)));
            }
          }
          for (Map.Entry<String, JsonObject> entry : tableToRowEntry.entrySet()) {
            tableToRowEntries
                .computeIfAbsent(entry.getKey(), k -> new ArrayList<JsonObject>())
                .add(entry.getValue());
          }
        }

        for (Map.Entry<String, List<JsonObject>> entry : tableToRowEntries.entrySet()) {
          long count =
              MilvusClientUtils.upsert(
                  client, databaseName, entry.getKey(), entry.getValue(), ids, fields, pathSystem);
          LOGGER.info("complete insertRows, insertCount:" + count);
        }
        cnt += size;
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return e;
    }
    return null;
  }

  @Override
  public boolean testConnection(StorageEngineMeta meta) {
//    MilvusClientV2 client = this.milvusClientV2Pool.getClient(DEFAULT_KEY);
    try {
      ConnectConfig config =
              ConnectConfig.builder().uri("grpc://127.0.0.1:19530").build();
      MilvusClientV2 client = new MilvusClientV2(config);
      return client != null;
    }catch(Exception e){
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executeProjectWithFilter(project, filter, dataArea);
  }

  private boolean filterContainsType(List<FilterType> types, Filter filter) {
    boolean res = false;
    if (types.contains(filter.getType())) {
      return true;
    }
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          res |= filterContainsType(types, child);
        }
        break;
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          res |= filterContainsType(types, child);
        }
        break;
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        res |= filterContainsType(types, notChild);
        break;
      default:
        break;
    }
    return res;
  }

  private TaskExecuteResult executeProjectWithFilter(
      Project project, Filter filter, DataArea dataArea) {
    MilvusClientV2 client = null;
    try {
      String databaseName = dataArea.getStorageUnit();

      client = this.getClient(databaseName);
      if (client == null) {
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }

      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(databaseName, s -> new MilvusPathSystem(databaseName));
      Map<String, Set<String>> collectionToFields =
          MilvusClientUtils.determinePaths(
              client, project.getPatterns(), project.getTagFilter(), false, pathSystem);
      List<cn.edu.tsinghua.iginx.vectordb.entity.Column> columns = new ArrayList<>();
      for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
        String collectionName = entry.getKey();
        Set<String> fields = entry.getValue();
        columns.addAll(
            MilvusClientUtils.query(
                client,
                databaseName,
                collectionName,
                new ArrayList<>(fields),
                filter,
                null,
                pathSystem));
      }
      return new TaskExecuteResult(new VectorDBQueryRowStream(columns, filter), null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("execute project task in milvus failure"), e));
    } finally {
      if (client != null) {
        this.milvusClientV2Pool.returnClient(DEFAULT_KEY, client);
      }
    }
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executeProjectDummyWithFilter(project, filter);
  }

  private TaskExecuteResult executeProjectDummyWithFilter(Project project, Filter filter) {
    MilvusClientV2 client = null;
    try {
      client = this.milvusClientV2Pool.getClient(DEFAULT_KEY);

      PathSystem pathSystem = new MilvusPathSystem("");
      Map<String, Set<String>> collectionToFields =
          MilvusClientUtils.determinePaths(
              client, project.getPatterns(), project.getTagFilter(), true, pathSystem);
      List<cn.edu.tsinghua.iginx.vectordb.entity.Column> columns = new ArrayList<>();
      for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
        String collectionName = entry.getKey();
        String databaseName = collectionName.substring(0, collectionName.indexOf("."));
        collectionName = collectionName.substring(collectionName.indexOf(".") + 1);
        Set<String> fields = entry.getValue();
        columns.addAll(
            MilvusClientUtils.query(
                client,
                databaseName,
                collectionName,
                new ArrayList<>(fields),
                filter,
                project.getPatterns(),
                pathSystem));
      }
      return new TaskExecuteResult(new VectorDBQueryRowStream(columns, filter), null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("execute project task in milvus failure"), e));
    } finally {
      if (client != null) {
        this.milvusClientV2Pool.returnClient(DEFAULT_KEY, client);
      }
    }
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeProjectWithFilter(project, select.getFilter(), dataArea);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeProjectDummyWithFilter(project, select.getFilter());
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    MilvusClientV2 client = null;
    try {
      String databaseName = dataArea.getStorageUnit();
      client = this.milvusClientV2Pool.getClient(DEFAULT_KEY);

      List<String> paths = delete.getPatterns();
      TagFilter tagFilter = delete.getTagFilter();

      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(databaseName, s -> new MilvusPathSystem(databaseName));
      if (delete.getKeyRanges() == null || delete.getKeyRanges().isEmpty()) {
        if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
          dropDatabase(client, databaseName);
        } else {
          Map<String, Set<String>> collectionToFields =
              MilvusClientUtils.determinePaths(client, paths, tagFilter, pathSystem);

          for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
            String collectionName = entry.getKey();
            Set<String> fields = entry.getValue();
            dropFields(client, collectionName, fields);
          }
        }
      } else {
        Map<String, Set<String>> collectionToFields =
            MilvusClientUtils.determinePaths(client, paths, tagFilter, pathSystem);

        for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
          String collectionName = entry.getKey();
          Set<String> fields = entry.getValue();

          for (KeyRange keyRange : delete.getKeyRanges()) {
            deleteFieldsByRange(client, collectionName, fields, keyRange, pathSystem);
          }
        }
      }
      return new TaskExecuteResult(null, null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalException(String.format("execute delete task in milvus failure"), e));
    } finally {
      if (client != null) {
        this.milvusClientV2Pool.returnClient(DEFAULT_KEY, client);
      }
    }
  }

  private MilvusClientV2 getClient(String databaseName) {
    MilvusClientV2 client = this.milvusClientV2Pool.getClient(DEFAULT_KEY);
    try {
      client.useDatabase(databaseName);
      return client;
    } catch (IllegalArgumentException e) {
      client.createDatabase(CreateDatabaseReq.builder().databaseName(databaseName).build());
      try {
        client.useDatabase(databaseName);
        return client;
      } catch (Exception e1) {
        LOGGER.error("Caught a RuntimeException:", e1);
      }
    } catch (Exception e) {
      LOGGER.error("Caught a RuntimeException:", e);
    }
    return null;
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    MilvusClientV2 client = null;
    try {
      DataView dataView = insert.getData();
      String databaseName = dataArea.getStorageUnit();

      client = this.getClient(databaseName);
      if (client == null) {
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }

      Exception e = null;
      switch (dataView.getRawDataType()) {
        case Row:
        case NonAlignedRow:
          //                e = insertNonAlignedRowRecords(client, databaseName, (RowDataView)
          // dataView);
          //                break;
        case Column:
        case NonAlignedColumn:
          e = insertRecords(client, databaseName, dataView);
          break;
      }
      if (e != null) {
        return new TaskExecuteResult(
            null, new PhysicalException(String.format("execute insert task in milvus failure"), e));
      }

      return new TaskExecuteResult(null, null);
    } finally {
      if (client != null) {
        this.milvusClientV2Pool.returnClient(DEFAULT_KEY, client);
      }
    }
  }

  @Override
  public List<Column> getColumns(Set<String> patterns, TagFilter tagFilter)
      throws PhysicalException {
    MilvusClientV2 client = null;
    try {
      client = this.milvusClientV2Pool.getClient(DEFAULT_KEY);
      if (patterns == null) {
        patterns = new HashSet<>();
      }
      if (patterns.size() == 0) {
        patterns.add("*");
      }

      PathSystem pathSystem = new MilvusPathSystem("");
      PathUtils.initAll(client, pathSystem);
      List<Column> columns = new ArrayList<>();
      for (String pattern : patterns) {
        List<String> list = PathUtils.getPathSystem(client, pathSystem).findPaths(pattern, null);
        for (String p : list) {
          Pair<String, Map<String, String>> pair = splitFullName(getPathAndVersion(p).getK());
          if (tagFilter != null && !TagKVUtils.match(pair.getV(), tagFilter)) {
            continue;
          }
          columns.add(PathUtils.getPathSystem(client, pathSystem).getColumn(p));
        }
      }
      return columns;
    } finally {
      if (client != null) {
        milvusClientV2Pool.returnClient(DEFAULT_KEY, client);
      }
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    MilvusClientV2 client = null;
    try {
      ColumnsInterval columnsInterval;
      TreeSet<String> paths = new TreeSet<>();
      client = this.milvusClientV2Pool.getClient(DEFAULT_KEY);

      PathSystem pathSystem = new MilvusPathSystem("");
      PathUtils.initAll(client, pathSystem);
      for (Column c : PathUtils.getPathSystem(client, pathSystem).getColumns().values()) {
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(prefix)
            && org.apache.commons.lang3.StringUtils.isNotEmpty(c.getPath())
            && !c.getPath().startsWith(prefix)) {
          continue;
        }
        paths.add(c.getPath());
      }

      if (paths.isEmpty()) {
        throw new PhysicalException("no data!");
      }

      if (prefix != null) {
        columnsInterval = new ColumnsInterval(prefix);
      } else {
        columnsInterval = new ColumnsInterval(paths.first(), StringUtils.nextString(paths.last()));
      }

      return new Pair<>(columnsInterval, KeyInterval.getDefaultKeyInterval());
    } finally {
      if (client != null) {
        this.milvusClientV2Pool.returnClient(DEFAULT_KEY, client);
      }
    }
  }

  @Override
  public void release() throws PhysicalException {
    this.milvusClientV2Pool.clear();
    this.milvusClientV2Pool.close();
  }
}
