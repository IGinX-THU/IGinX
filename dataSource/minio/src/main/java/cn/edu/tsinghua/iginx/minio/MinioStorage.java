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
package cn.edu.tsinghua.iginx.minio;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.minio.common.Filters;
import cn.edu.tsinghua.iginx.minio.common.MinioCalls;
import cn.edu.tsinghua.iginx.minio.kv.ObjectKvManager;
import cn.edu.tsinghua.iginx.minio.nio.S3FileSystem;
import cn.edu.tsinghua.iginx.minio.struct.DataBoundary;
import cn.edu.tsinghua.iginx.minio.struct.DataTarget;
import cn.edu.tsinghua.iginx.minio.struct.ObjectManager;
import cn.edu.tsinghua.iginx.minio.tree.ObjectTreeConfig;
import cn.edu.tsinghua.iginx.minio.tree.ObjectTreeManager;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import io.reactivex.rxjava3.core.Flowable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MinIO (S3 compatible object storage) data source.
 *
 * <ul>
 *   <li>dummy data: existing objects are read with the FileTree structure by {@link
 *       ObjectTreeManager} (raw, csv, parquet...), through a read-only NIO file system over S3. The
 *       ObjectKV structure is also supported, which reads objects written in the layout of data.
 *   <li>data written by IGinX: stored in a dedicated bucket with {@link ObjectKvManager}, one
 *       directory per storage unit.
 * </ul>
 */
public class MinioStorage implements IStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(MinioStorage.class);

  static final String IGINX_DATA_PREFIX = "iginx_";

  private static final int IO_THREADS = 16;

  private final MinioConfig config;
  private final OkHttpClient httpClient;
  private final MinioClient client;
  private final ExecutorService executor;
  private final ConcurrentHashMap<String, ObjectKvManager> dataManagers = new ConcurrentHashMap<>();
  @Nullable private final S3FileSystem dummyFileSystem;
  @Nullable private final ObjectManager dummyManager;

  public MinioStorage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(StorageEngineType.minio)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    this.config = MinioConfig.of(meta);
    this.httpClient = newHttpClient(config);
    this.client = newClient(config, httpClient);
    this.executor =
        new ThreadPoolExecutor(
            IO_THREADS,
            IO_THREADS,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("minio-io-%d").setDaemon(true).build());
    ((ThreadPoolExecutor) executor).allowCoreThreadTimeOut(true);

    try {
      if (!config.isReadOnly()) {
        ensureBucket(config.getDataBucket());
        for (String unit : listDataUnits()) {
          getDataManager(unit);
        }
      }
      if (config.isHasData()) {
        if (config.isObjectKv()) {
          this.dummyFileSystem = null;
          this.dummyManager =
              new ObjectKvManager(
                  client,
                  config.getDummyBucket(),
                  config.getDummyDir(),
                  executor,
                  config.getCompactThreshold());
        } else {
          this.dummyFileSystem =
              new S3FileSystem(
                  client,
                  Collections.singleton(config.getDataBucket()),
                  config.getReadBlockSize(),
                  Duration.ofMillis(config.getMetaCacheTtlMs()));
          this.dummyManager = newObjectTreeManager(dummyFileSystem, config, executor);
        }
      } else {
        this.dummyFileSystem = null;
        this.dummyManager = null;
      }
    } catch (IOException | RuntimeException e) {
      release(httpClient, executor);
      throw new StorageInitializationException("failed to initialize minio storage: " + e, e);
    }
  }

  private static OkHttpClient newHttpClient(MinioConfig config) {
    return new OkHttpClient.Builder()
        .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
        .readTimeout(config.getRequestTimeoutMs(), TimeUnit.MILLISECONDS)
        .writeTimeout(config.getRequestTimeoutMs(), TimeUnit.MILLISECONDS)
        .retryOnConnectionFailure(true)
        .build();
  }

  private static MinioClient newClient(MinioConfig config, OkHttpClient httpClient) {
    MinioClient.Builder builder =
        MinioClient.builder()
            .endpoint(config.getEndpointHost(), config.getEndpointPort(), config.isSecure())
            .credentials(config.getUsername(), config.getPassword())
            .httpClient(httpClient);
    if (config.getRegion() != null) {
      builder.region(config.getRegion());
    }
    return builder.build();
  }

  private static ObjectManager newObjectTreeManager(
      S3FileSystem fs, MinioConfig config, ExecutorService executor) throws IOException {
    Path root = fs.root();
    if (config.getDummyBucket() != null) {
      root = root.resolve(config.getDummyBucket());
      if (!config.getDummyDir().isEmpty()) {
        root = root.resolve(config.getDummyDir());
      }
    }
    return new ObjectTreeManager(root, ObjectTreeConfig.of(config.getDummyConfig()), executor);
  }

  private void ensureBucket(String bucket) throws IOException {
    boolean exists =
        MinioCalls.call(
            () -> client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build()));
    if (!exists) {
      LOGGER.info("creating bucket {} for iginx data", bucket);
      try {
        MinioCalls.run(() -> client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build()));
      } catch (IOException e) {
        boolean created =
            MinioCalls.call(
                () -> client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build()));
        if (!created) {
          throw e;
        }
      }
    }
  }

  private List<String> listDataUnits() throws IOException {
    List<String> units = new ArrayList<>();
    for (Result<Item> result :
        client.listObjects(
            ListObjectsArgs.builder()
                .bucket(config.getDataBucket())
                .prefix(IGINX_DATA_PREFIX)
                .recursive(false)
                .build())) {
      Item item = MinioCalls.call(result::get);
      if (item.isDir()) {
        String name = item.objectName();
        units.add(name.substring(IGINX_DATA_PREFIX.length(), name.length() - 1));
      }
    }
    return units;
  }

  private ObjectKvManager getDataManager(String unit) {
    if (unit.isEmpty() || unit.contains("..") || unit.contains("/") || unit.contains("\\")) {
      throw new IllegalArgumentException("invalid unit name: " + unit);
    }
    return dataManagers.computeIfAbsent(
        unit,
        u ->
            new ObjectKvManager(
                client,
                config.getDataBucket(),
                IGINX_DATA_PREFIX + u,
                executor,
                config.getCompactThreshold()));
  }

  @Override
  public boolean testConnection(StorageEngineMeta meta) {
    OkHttpClient testHttpClient = null;
    try {
      MinioConfig testConfig = MinioConfig.of(meta);
      testHttpClient = newHttpClient(testConfig);
      MinioClient testClient = newClient(testConfig, testHttpClient);
      MinioCalls.call(testClient::listBuckets);
      return true;
    } catch (Exception e) {
      LOGGER.error("failed to connect minio {}:{}", meta.getIp(), meta.getPort(), e);
      return false;
    } finally {
      if (testHttpClient != null) {
        testHttpClient.dispatcher().executorService().shutdown();
        testHttpClient.connectionPool().evictAll();
      }
    }
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    return executeQuery(dataArea, getDataTargetOf(null, project, dataArea), false);
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    return executeQuery(dataArea, getDataTargetOf(null, project, dataArea), true);
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeQuery(dataArea, getDataTargetOf(select, project, dataArea), false);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeQuery(dataArea, getDataTargetOf(select, project, dataArea), true);
  }

  @Override
  public TaskExecuteResult executeProjectWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectWithAgg(Project project, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAgg(
      Project project, Operator agg, DataArea dataArea) {
    return null;
  }

  private static DataTarget getDataTargetOf(
      @Nullable Select select, Project project, DataArea dataArea) {
    Filter filter = Filters.toFilter(dataArea.getKeyInterval());
    if (select != null) {
      filter = Filters.nullableAnd(filter, select.getFilter());
    }
    return new DataTarget(filter, project.getPatterns(), project.getTagFilter());
  }

  private TaskExecuteResult executeQuery(DataArea dataArea, DataTarget target, boolean dummy) {
    try {
      ObjectManager manager;
      if (dummy) {
        if (dummyManager == null) {
          throw new PhysicalTaskExecuteFailureException("dummy data is not configured");
        }
        manager = dummyManager;
      } else {
        manager = getDataManager(dataArea.getStorageUnit());
      }
      RowStream stream = manager.query(target);
      return new TaskExecuteResult(stream);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    } catch (IOException | RuntimeException e) {
      LOGGER.error("failed to query minio", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("failed to query minio: " + e, e));
    }
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    try {
      Filter filter = Filters.toFilter(delete.getKeyRanges());
      getDataManager(dataArea.getStorageUnit())
          .delete(new DataTarget(filter, delete.getPatterns(), delete.getTagFilter()));
      return new TaskExecuteResult();
    } catch (IOException | RuntimeException e) {
      LOGGER.error("failed to delete data in minio", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("failed to delete data in minio: " + e, e));
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    try {
      getDataManager(dataArea.getStorageUnit()).insert(insert.getData());
      return new TaskExecuteResult();
    } catch (IOException | RuntimeException e) {
      LOGGER.error("failed to insert data into minio", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("failed to insert data into minio: " + e, e));
    }
  }

  @Override
  public Flowable<Column> getColumns(Set<String> patterns, TagFilter tagFilter)
      throws PhysicalException {
    List<String> patternList =
        patterns == null || patterns.isEmpty() ? null : new ArrayList<>(patterns);
    Map<String, Column> columns = new LinkedHashMap<>();
    try {
      if (!config.isReadOnly()) {
        for (String unit : listDataUnits()) {
          DataTarget target = new DataTarget(new BoolFilter(false), patternList, tagFilter);
          try (RowStream stream = getDataManager(unit).query(target)) {
            for (Field field : stream.getHeader().getFields()) {
              columns.putIfAbsent(
                  field.getFullName(),
                  new Column(field.getName(), field.getType(), field.getTags(), false));
            }
          }
        }
      }
      if (dummyManager != null) {
        DataTarget target = new DataTarget(new BoolFilter(false), patternList, tagFilter);
        try (RowStream stream = dummyManager.query(target)) {
          for (Field field : stream.getHeader().getFields()) {
            columns.putIfAbsent(
                "dummy:" + field.getFullName(),
                new Column(field.getName(), field.getType(), field.getTags(), true));
          }
        }
      }
    } catch (IOException | RuntimeException e) {
      throw new PhysicalTaskExecuteFailureException("failed to get columns of minio: " + e, e);
    }
    return Flowable.fromIterable(columns.values());
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    if (dummyManager == null) {
      throw new PhysicalTaskExecuteFailureException("no data");
    }
    DataBoundary boundary;
    try {
      boundary = dummyManager.getBoundary(prefix == null || prefix.isEmpty() ? null : prefix);
    } catch (IOException | RuntimeException e) {
      throw new PhysicalTaskExecuteFailureException("failed to get boundary of minio: " + e, e);
    }
    if (boundary.isEmpty()) {
      throw new PhysicalTaskExecuteFailureException("no data");
    }
    return new Pair<>(boundary.getColumnsInterval(), boundary.getKeyInterval());
  }

  @Override
  public void release() throws PhysicalException {
    try {
      if (dummyManager != null) {
        dummyManager.close();
      }
    } catch (IOException e) {
      LOGGER.warn("failed to close dummy manager", e);
    }
    if (dummyFileSystem != null) {
      dummyFileSystem.close();
    }
    release(httpClient, executor);
  }

  private static void release(OkHttpClient httpClient, ExecutorService executor) {
    executor.shutdown();
    httpClient.dispatcher().executorService().shutdown();
    httpClient.connectionPool().evictAll();
  }
}
