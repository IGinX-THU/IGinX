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
package cn.edu.tsinghua.iginx.s3;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.descriptor.DriverDescriptor;
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
import cn.edu.tsinghua.iginx.s3.common.Filters;
import cn.edu.tsinghua.iginx.s3.common.S3Client;
import cn.edu.tsinghua.iginx.s3.common.S3Object;
import cn.edu.tsinghua.iginx.s3.kv.ObjectKvManager;
import cn.edu.tsinghua.iginx.s3.nio.S3FileSystem;
import cn.edu.tsinghua.iginx.s3.strategy.S3Strategy;
import cn.edu.tsinghua.iginx.s3.strategy.S3StrategyFactory;
import cn.edu.tsinghua.iginx.s3.struct.DataBoundary;
import cn.edu.tsinghua.iginx.s3.struct.DataTarget;
import cn.edu.tsinghua.iginx.s3.struct.ObjectManager;
import cn.edu.tsinghua.iginx.s3.tree.ObjectTreeConfig;
import cn.edu.tsinghua.iginx.s3.tree.ObjectTreeManager;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 compatible object storage data source. The service (MinIO, Aliyun OSS...) is selected by the
 * {@code engine} param, see {@link S3StrategyFactory}.
 *
 * <ul>
 *   <li>dummy data: existing objects are read through a read-only NIO file system over S3 by {@link
 *       ObjectTreeManager}, as raw bytes with the LegacyFilesystem structure by default, or parsed
 *       by their extensions (raw, csv, parquet...) with the FileTree structure. The ObjectKV
 *       structure is also supported, which reads objects written in the layout of data.
 *   <li>data written by IGinX: stored in a dedicated bucket with {@link ObjectKvManager}, one
 *       directory per storage unit.
 * </ul>
 */
@DriverDescriptor(S3DriverDescriptor.class)
public class S3Storage implements IStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3Storage.class);

  static final String IGINX_DATA_PREFIX = "iginx_";

  private static final int IO_THREADS = 16;

  /**
   * Must be shorter than the idle timeout of the server (30s for MinIO), otherwise a request may be
   * sent on a connection that the server is closing and fail with "Connection reset".
   */
  private static final long KEEP_ALIVE_SECONDS = 20;

  private final S3Config config;
  private final S3Strategy strategy;
  private final OkHttpClient httpClient;
  private final S3Client client;
  private final ExecutorService executor;
  private final ConcurrentHashMap<String, ObjectKvManager> dataManagers = new ConcurrentHashMap<>();
  @Nullable private final S3FileSystem dummyFileSystem;
  @Nullable private final ObjectManager dummyManager;

  public S3Storage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(StorageEngineType.s3)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    this.config = S3Config.of(meta);
    this.strategy = S3StrategyFactory.create(config);
    strategy.validate();
    this.httpClient = newHttpClient(config);
    this.client = strategy.createClient(httpClient);
    this.executor =
        new ThreadPoolExecutor(
            IO_THREADS,
            IO_THREADS,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("s3-io-%d").setDaemon(true).build());
    ((ThreadPoolExecutor) executor).allowCoreThreadTimeOut(true);

    try {
      if (!config.isReadOnly()) {
        strategy.ensureBucket(client, config.getDataBucket());
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
      throw new StorageInitializationException("failed to initialize s3 storage: " + e, e);
    }
  }

  /**
   * Always pass our own http client to the SDK: the default one loads certificates from
   * SSL_CERT_FILE and fails if that file does not exist.
   */
  private static OkHttpClient newHttpClient(S3Config config) {
    return new OkHttpClient.Builder()
        .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
        .readTimeout(config.getRequestTimeoutMs(), TimeUnit.MILLISECONDS)
        .writeTimeout(config.getRequestTimeoutMs(), TimeUnit.MILLISECONDS)
        .retryOnConnectionFailure(true)
        .connectionPool(new ConnectionPool(IO_THREADS, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS))
        .build();
  }

  private static ObjectManager newObjectTreeManager(
      S3FileSystem fs, S3Config config, ExecutorService executor) throws IOException {
    Path root = fs.root();
    if (config.getDummyBucket() != null) {
      root = root.resolve(config.getDummyBucket());
      if (!config.getDummyDir().isEmpty()) {
        root = root.resolve(config.getDummyDir());
      }
    }
    ObjectTreeConfig treeConfig = ObjectTreeConfig.of(config.getDummyConfig());
    if (config.isLegacyFilesystem()) {
      return ObjectTreeManager.legacyFilesystem(root, treeConfig, executor);
    }
    return new ObjectTreeManager(root, treeConfig, executor);
  }

  private List<String> listDataUnits() throws IOException {
    List<String> units = new ArrayList<>();
    for (S3Object object : client.listObjects(config.getDataBucket(), IGINX_DATA_PREFIX, false)) {
      if (object.isDir()) {
        String name = object.key();
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

  /**
   * Only accesses the buckets in params: users of public clouds may not be permitted to list
   * buckets. An existing bucket is not required, but the credentials must be accepted.
   */
  @Override
  public boolean testConnection(StorageEngineMeta meta) {
    OkHttpClient testHttpClient = null;
    try {
      S3Config testConfig = S3Config.of(meta);
      S3Strategy testStrategy = S3StrategyFactory.create(testConfig);
      testStrategy.validate();
      testHttpClient = newHttpClient(testConfig);
      S3Client testClient = testStrategy.createClient(testHttpClient);
      if (!testConfig.isReadOnly()) {
        testClient.bucketExists(testConfig.getDataBucket());
      } else if (testConfig.getDummyBucket() != null) {
        testClient.bucketExists(testConfig.getDummyBucket());
      } else {
        testClient.listBuckets();
      }
      return true;
    } catch (Exception e) {
      LOGGER.error("failed to connect s3 {}:{}", meta.getIp(), meta.getPort(), e);
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
      LOGGER.error("failed to query s3", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("failed to query s3: " + e, e));
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
      LOGGER.error("failed to delete data in s3", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("failed to delete data in s3: " + e, e));
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    try {
      getDataManager(dataArea.getStorageUnit()).insert(insert.getData());
      return new TaskExecuteResult();
    } catch (IOException | RuntimeException e) {
      LOGGER.error("failed to insert data into s3", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("failed to insert data into s3: " + e, e));
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
      throw new PhysicalTaskExecuteFailureException("failed to get columns of s3: " + e, e);
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
      throw new PhysicalTaskExecuteFailureException("failed to get boundary of s3: " + e, e);
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
