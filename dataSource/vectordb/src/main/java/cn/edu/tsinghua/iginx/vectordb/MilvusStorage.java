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
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
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
import cn.edu.tsinghua.iginx.vectordb.pool.MilvusConnectPool;
import cn.edu.tsinghua.iginx.vectordb.pool.MilvusConnectPoolConfig;
import cn.edu.tsinghua.iginx.vectordb.support.PathSystem;
import cn.edu.tsinghua.iginx.vectordb.support.impl.MilvusPathSystem;
import cn.edu.tsinghua.iginx.vectordb.tools.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.UpsertResp;
import io.reactivex.rxjava3.core.Flowable;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MilvusStorage implements IStorage {

  private MilvusConnectPool milvusConnectPool;

  private static final Logger LOGGER = LoggerFactory.getLogger(MilvusStorage.class);

  Map<String, PathSystem> pathSystemMap = new ConcurrentHashMap<>();

  public Map<String, PathSystem> getPathSystemMap() {
    return pathSystemMap;
  }

  private int topK = 1;

  /**
   * 构造函数，用于初始化 MilvusStorage 实例。
   *
   * @param meta 存储引擎的元数据。
   * @throws StorageInitializationException 如果存储引擎类型不匹配或初始化过程中发生错误。
   */
  public MilvusStorage(StorageEngineMeta meta) throws StorageInitializationException {
    LOGGER.info("init milvus storage {} : {}", meta.getIp(), meta.getPort());
    if (!meta.getStorageEngine().equals(StorageEngineType.vectordb)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    Map<String, String> params = meta.getExtraParams();
    String protocol = params.getOrDefault(DB_PROTOCOL, DEFAULT_DB_PROTOCOL);
    int maxTotal =
        Integer.parseInt(params.getOrDefault(MAX_TOTAL, String.valueOf(DEFAULT_MAX_TOTAL)));
    int maxIdle = Integer.parseInt(params.getOrDefault(MAX_IDLE, String.valueOf(DEFAULT_MAX_IDLE)));
    int minIdle = Integer.parseInt(params.getOrDefault(MIN_IDLE, String.valueOf(DEFAULT_MIN_IDLE)));
    topK = Integer.parseInt(params.getOrDefault(TOP_K, String.valueOf(DEFAULT_TOP_K)));

    MilvusConnectPoolConfig config =
        new MilvusConnectPoolConfig(
            meta.getIp(),
            meta.getPort(),
            protocol,
            params.get("username"),
            params.get("password"),
            maxIdle,
            minIdle,
            maxTotal);
    this.milvusConnectPool = config.milvusConnectPool();
  }

  private Exception insertRecords(MilvusClientV2 client, String databaseName, DataView data) {
    int batchSize = Math.min(data.getKeySize(), 10000);
    try {
      Set<String> collections = new HashSet<>();
      collections.addAll(client.listCollections().getCollectionNames());

      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(databaseName, s -> new MilvusPathSystem(databaseName));

      int colIndex[] = new int[data.getKeySize()];
      ExecutorCompletionService<UpsertResp> completionService =
          new ExecutorCompletionService<>(TaskExecutor.getExecutorService());
      int taskCount = 0;
      for (int j = 0; j < data.getPathNum(); j++) {
        String path = data.getPaths().get(j);
        Map<String, String> tags = new HashMap<>();
        if (data.getTagsList() != null && !data.getTagsList().isEmpty()) {
          tags = data.getTagsList().get(j);
        }
        Pair<String, String> collectionAndField =
            PathUtils.getCollectionAndFieldByPath(path, tags, false);
        DataType dataType = data.getDataType(j);
        String collectionName =
            collectionAndField.getK() + "." + collectionAndField.getV() + "[[" + dataType + "]]";

        if (!collections.contains(NameUtils.escape(collectionName))) {
          MilvusClientUtils.createCollection(
              client, databaseName, collectionName, DataType.LONG, data.getDataType(j), pathSystem);
        }

        int cnt = 0;
        int rowIndex = 0;
        while (cnt < data.getKeySize()) {
          int size = Math.min(data.getKeySize() - cnt, batchSize);
          List<JsonObject> rowData = new ArrayList<>();
          for (int i = cnt; i < cnt + size; i++) {
            JsonObject row = new JsonObject();
            Object obj;
            if (data instanceof RowDataView) {
              if (data.getBitmapView(i).get(j)) {
                obj = data.getValue(i, colIndex[i]++);
              } else {
                obj = null;
              }
            } else {
              if (data.getBitmapView(j).get(i)) {
                obj = data.getValue(j, rowIndex++);
              } else {
                obj = null;
              }
            }

            if (obj != null
                && MilvusClientUtils.addProperty(row, MILVUS_DATA_FIELD_NAME, obj, dataType)) {
              row.addProperty(MILVUS_PRIMARY_FIELD_NAME, data.getKey(i));
              row.add(
                  MILVUS_VECTOR_FIELD_NAME,
                  new Gson().toJsonTree(CommonUtils.generateFloatVector(DEFAULT_DIMENSION)));
              rowData.add(row);
            }
          }

          if (rowData.size() > 0) {
            Callable<UpsertResp> task =
                () -> {
                  try (MilvusPoolClient milvusPoolClient =
                      new MilvusPoolClient(this.milvusConnectPool)) {
                    MilvusClientV2 c = milvusPoolClient.getClient();
                    c.useDatabase(NameUtils.escape(databaseName));
                    return c.upsert(
                        UpsertReq.builder()
                            .collectionName(NameUtils.escape(collectionName))
                            .data(rowData)
                            .build());
                  }
                };
            completionService.submit(task);
            taskCount++;
          }
          cnt += size;
        }
      }

      for (int i = 0; i < taskCount; i++) {
        try {
          Future<UpsertResp> resp = completionService.take();
          if (resp != null && resp.get() != null) {
            LOGGER.info("upsert result: " + resp.get());
          } else {
            LOGGER.error("upsert error");
          }
        } catch (Exception e) {
          LOGGER.error("unexpected error: ", e);
          return e;
        }
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return e;
    }
    return null;
  }

  @Override
  public boolean testConnection(StorageEngineMeta meta) {
    try (MilvusClient client = new MilvusClient(meta)) {
      LOGGER.info("milvus test connection is successful.");
      return true;
    } catch (Exception e) {
      this.pathSystemMap = new ConcurrentHashMap<>();
      LOGGER.error("milvus test connection error : ", e);
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
    try (MilvusPoolClient milvusClient = new MilvusPoolClient(this.milvusConnectPool)) {
      MilvusClientV2 client = milvusClient.getClient();
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
              client, patterns, project.getTagFilter(), false, pathSystem);
      List<cn.edu.tsinghua.iginx.vectordb.entity.Column> columns = new ArrayList<>();

      ExecutorCompletionService<List<cn.edu.tsinghua.iginx.vectordb.entity.Column>>
          completionService = new ExecutorCompletionService<>(TaskExecutor.getExecutorService());
      for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
        String collectionName = entry.getKey();
        Set<String> fields = entry.getValue();
        Callable<List<cn.edu.tsinghua.iginx.vectordb.entity.Column>> task =
            () -> {
              try (MilvusPoolClient c = new MilvusPoolClient(this.milvusConnectPool)) {
                return MilvusClientUtils.query(
                    c.getClient(),
                    databaseName,
                    collectionName,
                    new ArrayList<>(fields),
                    filter,
                    null,
                    pathSystem,
                    topK);
              }
            };
        completionService.submit(task);
      }

      for (int i = 0; i < collectionToFields.size(); i++) {
        try {
          Future<List<cn.edu.tsinghua.iginx.vectordb.entity.Column>> future =
              completionService.take();
          columns.addAll(future.get());
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.error("Execute project task in milvus failure: ", e);
        }
      }

      return new TaskExecuteResult(new VectorDBQueryRowStream(columns, filter), null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("Execute project task in milvus failure : %s", e)));
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
    try (MilvusPoolClient milvusClient = new MilvusPoolClient(this.milvusConnectPool)) {
      MilvusClientV2 client = milvusClient.getClient();
      if (client == null) {
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(String.format("cannot connect to milvus")));
      }
      List<String> patterns = project.getPatterns();
      if (patterns == null) {
        patterns = Arrays.asList("*");
      }
      PathSystem pathSystem = pathSystemMap.computeIfAbsent("", s -> new MilvusPathSystem(""));
      Map<String, Set<String>> collectionToFields =
          MilvusClientUtils.determinePaths(
              client, patterns, project.getTagFilter(), true, pathSystem);
      List<cn.edu.tsinghua.iginx.vectordb.entity.Column> columns = new ArrayList<>();

      ExecutorCompletionService<List<cn.edu.tsinghua.iginx.vectordb.entity.Column>>
          completionService = new ExecutorCompletionService<>(TaskExecutor.getExecutorService());
      for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
        String collectionName = entry.getKey();
        String databaseName = collectionName.substring(0, collectionName.indexOf("."));
        collectionName = collectionName.substring(collectionName.indexOf(".") + 1);
        Set<String> fields = entry.getValue();

        String finalCollectionName = collectionName;
        List<String> finalPatterns = patterns;
        Callable<List<cn.edu.tsinghua.iginx.vectordb.entity.Column>> task =
            () -> {
              try (MilvusPoolClient c = new MilvusPoolClient(this.milvusConnectPool)) {
                return MilvusClientUtils.query(
                    c.getClient(),
                    databaseName,
                    finalCollectionName,
                    new ArrayList<>(fields),
                    filter,
                    finalPatterns,
                    pathSystem,
                    topK);
              }
            };
        completionService.submit(task);
      }

      for (int i = 0; i < collectionToFields.size(); i++) {
        try {
          Future<List<cn.edu.tsinghua.iginx.vectordb.entity.Column>> future =
              completionService.take();
          columns.addAll(future.get());
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.error("Execute project task in milvus failure : ", e);
        }
      }

      return new TaskExecuteResult(new VectorDBQueryRowStream(columns, filter), null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("Execute project task in milvus failure, %s", e)));
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
  public TaskExecuteResult executeProjectWithAggSelect(
      Project project, Select select, Operator operator, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAggSelect(
      Project project, Select select, Operator operator, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectWithAgg(
      Project project, Operator operator, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAgg(
      Project project, Operator operator, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    String databaseName = dataArea.getStorageUnit();
    try (MilvusPoolClient milvusClient = new MilvusPoolClient(this.milvusConnectPool)) {
      MilvusClientV2 client = milvusClient.getClient();
      List<String> paths = delete.getPatterns();
      TagFilter tagFilter = delete.getTagFilter();
      PathSystem pathSystem =
          pathSystemMap.computeIfAbsent(databaseName, s -> new MilvusPathSystem(databaseName));
      if (delete.getKeyRanges() == null
          || delete.getKeyRanges().isEmpty()
          || (delete.getKeyRanges().size() == 1
              && delete.getKeyRanges().get(0).getActualBeginKey() == 0
              && delete.getKeyRanges().get(0).getEndKey() == Long.MAX_VALUE)) {
        if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
          dropDatabase(client, databaseName);
          pathSystemMap.remove(databaseName);
        } else {
          MilvusClientUtils.useDatabase(client, databaseName);
          Map<String, Set<String>> collectionToFields =
              MilvusClientUtils.determinePaths(client, paths, tagFilter, pathSystem);

          ExecutorCompletionService<Boolean> completionService =
              new ExecutorCompletionService<>(TaskExecutor.getExecutorService());
          for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
            String collectionName = entry.getKey();
            Callable<Boolean> task =
                () -> {
                  try (MilvusPoolClient c = new MilvusPoolClient(this.milvusConnectPool)) {
                    pathSystem.deletePath(
                        PathUtils.getPathUnescaped(databaseName, collectionName, ""));
                    return MilvusClientUtils.dropCollection(
                        c.getClient(), databaseName, collectionName, entry.getValue());
                  } catch (Exception e) {
                    LOGGER.error("Execute delete task in milvus : ", e);
                  }
                  return false;
                };
            completionService.submit(task);
          }

          for (int i = 0; i < collectionToFields.size(); i++) {
            try {
              completionService.take();
            } catch (Exception e) {
              LOGGER.error("Execute delete task in milvus : ", e);
              return new TaskExecuteResult(
                  new PhysicalException(
                      String.format("Execute delete task in milvus failure: %s", e)));
            }
          }
        }
      } else {
        MilvusClientUtils.useDatabase(client, databaseName);
        Map<String, Set<String>> collectionToFields =
            MilvusClientUtils.determinePaths(client, paths, tagFilter, pathSystem);

        ExecutorCompletionService<Long> completionService =
            new ExecutorCompletionService<>(TaskExecutor.getExecutorService());
        for (Map.Entry<String, Set<String>> entry : collectionToFields.entrySet()) {
          String collectionName = entry.getKey();

          Callable<Long> task =
              () -> {
                try (MilvusPoolClient c = new MilvusPoolClient(this.milvusConnectPool)) {
                  long r = 0;
                  for (KeyRange keyRange : delete.getKeyRanges()) {
                    long deletedCount =
                        deleteByRange(
                            c.getClient(), databaseName, collectionName, keyRange, pathSystem);
                    r += deletedCount;
                  }
                  return r;
                }
              };
          completionService.submit(task);
        }

        for (int i = 0; i < collectionToFields.size(); i++) {
          try {
            completionService.take();
          } catch (Exception e) {
            LOGGER.error("Execute delete task in milvus : ", e);
            return new TaskExecuteResult(
                new PhysicalException(
                    String.format("Execute delete task in milvus failure: %s", e)));
          }
        }
      }
      return new TaskExecuteResult(null, null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalException(String.format("execute delete task in milvus failure: %s", e)));
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String databaseName = dataArea.getStorageUnit();
    try (MilvusPoolClient milvusClient = new MilvusPoolClient(this.milvusConnectPool)) {
      MilvusClientV2 client = milvusClient.getClient();
      DataView dataView = insert.getData();
      if (client == null) {
        return new TaskExecuteResult(
            new PhysicalTaskExecuteFailureException(
                String.format("cannot connect to database %s", databaseName)));
      }

      MilvusClientUtils.useDatabase(client, databaseName);
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
  public Flowable<Column> getColumns(Set<String> patterns, TagFilter tagFilter)
      throws PhysicalException {
    try (MilvusPoolClient milvusClient = new MilvusPoolClient(this.milvusConnectPool)) {
      MilvusClientV2 client = milvusClient.getClient();
      if (patterns == null || patterns.size() == 0) {
        patterns = new HashSet<>();
      }
      if (patterns.size() == 0) {
        patterns.add("*");
      }

      PathUtils.initStorage(client, this);
      Set<Column> columns = new HashSet<>();
      for (String pattern : patterns) {
        for (PathSystem pathSystem : pathSystemMap.values()) {
          List<String> list = PathUtils.getPathSystem(client, pathSystem).findPaths(pattern, null);
          for (String p : list) {
            Pair<String, Map<String, String>> pair = splitFullName(getPathAndVersion(p).getK());
            if (tagFilter != null && !TagKVUtils.match(pair.getV(), tagFilter)) {
              continue;
            }
            Column c = PathUtils.getPathSystem(client, pathSystem).getColumn(p);
            Column column = new Column(c.getPath(), c.getDataType(), c.getTags(), c.isDummy());
            columns.add(column);
          }
        }
      }
      return Flowable.fromIterable(columns);
    } catch (Exception e) {
      throw new PhysicalException(String.format("execute query task in milvus failure"));
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    try (MilvusPoolClient milvusClient = new MilvusPoolClient(this.milvusConnectPool)) {
      MilvusClientV2 client = milvusClient.getClient();
      ColumnsInterval columnsInterval;
      TreeSet<String> paths = new TreeSet<>();

      PathUtils.initStorage(client, this);
      for (PathSystem pathSystem : pathSystemMap.values()) {
        for (Column c : PathUtils.getPathSystem(client, pathSystem).getColumns().values()) {
          if (org.apache.commons.lang3.StringUtils.isNotEmpty(prefix)
              && org.apache.commons.lang3.StringUtils.isNotEmpty(c.getPath())
              && !c.getPath().startsWith(prefix)) {
            continue;
          }
          paths.add(c.getPath());
        }
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
    LOGGER.info("close milvus client pool ...");
    this.milvusConnectPool.close();
  }
}
