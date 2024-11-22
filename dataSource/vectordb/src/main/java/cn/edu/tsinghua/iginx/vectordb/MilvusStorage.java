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
import static cn.edu.tsinghua.iginx.vectordb.tools.MilvusClientUtils.*;
import static cn.edu.tsinghua.iginx.vectordb.tools.NameUtils.getPathAndVersion;
import static cn.edu.tsinghua.iginx.vectordb.tools.TagKVUtils.splitFullName;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
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
import io.milvus.v2.client.MilvusClientV2;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MilvusStorage implements IStorage {

  /** Milvus 客户端池，用于管理与 Milvus 服务的连接。 */
  //  private final MilvusClientV2Pool milvusClientV2Pool;

  private final StorageEngineMeta meta;

  private static final Logger LOGGER = LoggerFactory.getLogger(MilvusStorage.class);

  private static final String DEFAULT_KEY = "default";

  public static final int BATCH_SIZE = 10000;

  Map<String, PathSystem> pathSystemMap = new ConcurrentHashMap<>();

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
  }

  private Map<String, Map<String, String>> createOrAlterCollections(
      MilvusClientV2 client,
      String storageUnit,
      List<String> paths,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypeList)
      throws InterruptedException, UnsupportedEncodingException {
    MilvusClientUtils.useDatabase(client, storageUnit);
    Map<String, Set<String>> collectionToFields = new HashMap<>();
    Map<String, Map<String, DataType>> fieldToType = new HashMap<>();
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

      fieldToType
          .computeIfAbsent(collectionAndField.getK(), s -> new HashMap<>())
          .put(collectionAndField.getV(), dataTypeList.get(i));
    }

    Map<String, Map<String, String>> collectionToFieldsMap = new HashMap<>();
    for (String collection : collectionToFields.keySet()) {
      if (!collections.contains(collection)) {
        MilvusClientUtils.createCollection(client, storageUnit, collection, DataType.LONG);
      }

      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(storageUnit, s -> new MilvusPathSystem(storageUnit));
      Map<String, String> fields =
          MilvusClientUtils.addCollectionFields(
              client,
              storageUnit,
              collection,
              collectionToFields.get(collection),
              fieldToType.get(collection),
              pathSystem);
      collectionToFieldsMap.put(collection, fields);
    }
    return collectionToFieldsMap;
  }

  private Exception insertRecords(MilvusClientV2 client, String databaseName, DataView data) {
    int batchSize = Math.min(data.getKeySize(), BATCH_SIZE);
    try {
      Map<String, Map<String, String>> collectionToFieldsMap =
          createOrAlterCollections(
              client, databaseName, data.getPaths(), data.getTagsList(), data.getDataTypeList());

      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(databaseName, s -> new MilvusPathSystem(databaseName));
      // 插入数据
      Map<String, List<JsonObject>> tableToRowEntries = new HashMap<>(); // <表名, 插入数据>
      int cnt = 0;
      Set fields = new HashSet();
      int[] rowIndex = new int[data.getPathNum()];
      // 获取每个字段的属性
      String[] collectionNames = new String[data.getPathNum()];
      String[] columnNames = new String[data.getPathNum()];
      DataType[] dataTypes = new DataType[data.getPathNum()];
      for (int j = 0; j < data.getPathNum(); j++) {
        String path = data.getPath(j);
        DataType dataType = data.getDataType(j);
        Map<String, String> tags = data.getTags(j);
        path = PathUtils.getPathSystem(client, pathSystem).findPath(path, tags);
        String collectionName = path.substring(0, path.lastIndexOf("."));
        String columnName = path.substring(path.lastIndexOf(".") + 1);
        fields.add(columnName);
        collectionNames[j] = collectionName;
        // 修改成完整字段名称
        if (collectionToFieldsMap.containsKey(collectionName)
            && collectionToFieldsMap.get(collectionName).containsKey(columnName)) {
          columnNames[j] = collectionToFieldsMap.get(collectionName).get(columnName);
        } else {
          columnNames[j] = columnName;
        }

        dataTypes[j] = dataType;
      }
      while (cnt < data.getKeySize()) {
        int size = Math.min(data.getKeySize() - cnt, batchSize);
        Set idSet = new HashSet();
        for (int i = cnt; i < cnt + size; i++) {
          Map<String, JsonObject> tableToRowEntry = new HashMap<>();
          int colIndex = 0;
          for (int j = 0; j < data.getPathNum(); j++) {
            JsonObject row =
                tableToRowEntry.computeIfAbsent(collectionNames[j], k -> new JsonObject());
            Object obj;
            if (data instanceof RowDataView) {
              if (data.getBitmapView(i).get(j)) {
                obj = data.getValue(i, colIndex++);
              } else {
                obj = null;
              }
            } else {
              if (data.getBitmapView(j).get(i)) {
                obj = data.getValue(j, rowIndex[j]++);
              } else {
                obj = null;
              }
            }
            boolean added = MilvusClientUtils.addProperty(row, columnNames[j], obj, dataTypes[j]);
            if (added) {
              idSet.add(data.getKey(i));
              if (!row.has(MILVUS_PRIMARY_FIELD_NAME)) {
                row.addProperty(MILVUS_PRIMARY_FIELD_NAME, data.getKey(i));
                row.add(
                    MILVUS_VECTOR_FIELD_NAME,
                    new Gson().toJsonTree(CommonUtils.generateFloatVector(DEFAULT_DIMENSION)));
              }
            }
          }
          for (Map.Entry<String, JsonObject> entry : tableToRowEntry.entrySet()) {
            if (entry.getValue().size() > 0) {
              tableToRowEntries
                  .computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                  .add(entry.getValue());
            }
          }
        }
        List ids = new ArrayList<>();
        ids.addAll(idSet);
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
    //    return client != null;
    try (MilvusClient milvusClient = new MilvusClient(meta)) {
      return milvusClient.getClient() != null;
    } catch (Exception e) {
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

  private TaskExecuteResult executeProjectWithFilter(
      Project project, Filter filter, DataArea dataArea) {
    String databaseName = dataArea.getStorageUnit();
    try (MilvusClient client = new MilvusClient(meta)) {
      if (client == null) {
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }
      List<String> patterns = project.getPatterns();
      if (patterns == null) {
        patterns = Arrays.asList("*");
      }
      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(databaseName, s -> new MilvusPathSystem(databaseName));
      Map<String, Set<String>> collectionToFields =
          MilvusClientUtils.determinePaths(
              client.getClient(), patterns, project.getTagFilter(), false, pathSystem);
      List<cn.edu.tsinghua.iginx.vectordb.entity.Column> columns = new ArrayList<>();
      for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
        String collectionName = entry.getKey();
        Set<String> fields = entry.getValue();
        columns.addAll(
            MilvusClientUtils.query(
                client.getClient(),
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
              String.format("execute project task in milvus failure : {}"), e));
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
    try (MilvusClient milvusClient = new MilvusClient(meta)) {
      MilvusClientV2 client = milvusClient.getClient();
      if (client == null) {
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(String.format("cannot connect to milvus")));
      }
      List<String> patterns = project.getPatterns();
      if (patterns == null) {
        patterns = Arrays.asList("*");
      }
      PathSystem pathSystem = new MilvusPathSystem("");
      Map<String, Set<String>> collectionToFields =
          MilvusClientUtils.determinePaths(
              client, patterns, project.getTagFilter(), true, pathSystem);
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
                patterns,
                pathSystem));
      }
      return new TaskExecuteResult(new VectorDBQueryRowStream(columns, filter), null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("execute project task in milvus failure, %s"), e));
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
    String databaseName = dataArea.getStorageUnit();
    try (MilvusClient milvusClient = new MilvusClient(meta)) {
      MilvusClientV2 client = milvusClient.getClient();

      List<String> paths = delete.getPatterns();
      TagFilter tagFilter = delete.getTagFilter();

      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(databaseName, s -> new MilvusPathSystem(databaseName));
      if (delete.getKeyRanges() == null || delete.getKeyRanges().isEmpty()) {
        if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
          dropDatabase(client, databaseName);
        } else {
          MilvusClientUtils.useDatabase(client, databaseName);
          Map<String, Set<String>> collectionToFields =
              MilvusClientUtils.determinePaths(client, paths, tagFilter, pathSystem);

          for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
            String collectionName = entry.getKey();
            Set<String> fields = entry.getValue();
            dropFields(client, collectionName, fields);
            for (String field : fields) {
              pathSystem.deletePath(
                  PathUtils.getPathUnescaped(databaseName, collectionName, field));
            }
          }
        }
      } else {
        MilvusClientUtils.useDatabase(client, databaseName);
        LOGGER.info("delete by range : {} {}", paths, delete.getKeyRanges());
        Map<String, Set<String>> collectionToFields =
            MilvusClientUtils.determinePaths(client, paths, tagFilter, pathSystem);

        for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
          String collectionName = entry.getKey();
          Set<String> fields = entry.getValue();

          for (KeyRange keyRange : delete.getKeyRanges()) {
            LOGGER.info("delete by range : {} {} {}", databaseName, collectionName, keyRange);
            deleteFieldsByRange(client, collectionName, fields, keyRange, pathSystem);
          }
        }
      }
      return new TaskExecuteResult(null, null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalException(String.format("execute delete task in milvus failure: {}"), e));
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String databaseName = dataArea.getStorageUnit();
    try (MilvusClient milvusClient = new MilvusClient(meta)) {
      MilvusClientV2 client = milvusClient.getClient();
      DataView dataView = insert.getData();
      if (client == null) {
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }

      Exception e = null;
      switch (dataView.getRawDataType()) {
        case Row:
        case NonAlignedRow:
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
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(null, null);
    }
  }

  @Override
  public List<Column> getColumns(Set<String> patterns, TagFilter tagFilter)
      throws PhysicalException {
    try (MilvusClient milvusClient = new MilvusClient(meta)) {
      MilvusClientV2 client = milvusClient.getClient();
      if (patterns == null || patterns.size() == 0) {
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
    } catch (Exception e) {
      throw new PhysicalException(String.format("execute query task in milvus failure"));
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    try (MilvusClient milvusClient = new MilvusClient(meta)) {
      MilvusClientV2 client = milvusClient.getClient();
      ColumnsInterval columnsInterval;
      TreeSet<String> paths = new TreeSet<>();

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
    } catch (Exception e) {
      throw new PhysicalException(String.format("execute query task in milvus failure : %s", e));
    }
  }

  @Override
  public void release() throws PhysicalException {
    //    this.milvusClientV2Pool.clear();
    //    this.milvusClientV2Pool.close();
  }
}
