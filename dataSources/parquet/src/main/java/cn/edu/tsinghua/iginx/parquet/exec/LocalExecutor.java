/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.common.utils.FileUtils;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import cn.edu.tsinghua.iginx.parquet.manager.data.DataManager;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.DummyManager;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.EmptyManager;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExecutor implements Executor {

  private static final Logger logger = LoggerFactory.getLogger(LocalExecutor.class);

  public String dataDir;

  private final Map<String, Manager> managers = new ConcurrentHashMap<>();

  private final Manager dummyManager;

  public LocalExecutor(
      boolean hasData, boolean readOnly, String dataDir, String dummyDir, String dirPrefix)
      throws StorageInitializationException {
    testValidAndInit(hasData, readOnly, dataDir, dummyDir);

    if (hasData) {
      String embeddedPrefix;
      if (dirPrefix == null || dirPrefix.isEmpty()) {
        embeddedPrefix = FileUtils.getLastDirName(dataDir);
      } else {
        embeddedPrefix = dirPrefix;
      }
      dummyManager = new DummyManager(Paths.get(dummyDir), embeddedPrefix);
    } else {
      dummyManager = new EmptyManager();
    }

    recoverFromDisk();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    close();
                  } catch (Throwable e) {
                    logger.error("fail to close parquet executor", e);
                  }
                }));
  }

  // to test whether dummy_dir is dir and create data_dir if not exits
  private void testValidAndInit(
      boolean has_data, boolean read_only, String data_dir, String dummy_dir)
      throws StorageInitializationException {

    // has_data & read_only : dummy_dir required
    // has_data : dummy_dir and dir required
    // no data : dir required
    if (has_data) {
      if (dummy_dir == null || dummy_dir.isEmpty()) {
        throw new StorageInitializationException("Data dir not provided in dummy storage.");
      }
      File dummyFile = new File(dummy_dir);
      if (dummyFile.isFile()) {
        throw new StorageInitializationException(
            String.format("Dummy dir %s is not a directory.", dummy_dir));
      }
      if (!read_only) {
        if (data_dir == null || data_dir.isEmpty()) {
          throw new StorageInitializationException(
              "Data dir not provided in non-read-only storage.");
        }
        File dataFile = new File(data_dir);
        if (dataFile.isFile()) {
          throw new StorageInitializationException(
              String.format("Data dir %s is not a directory.", data_dir));
        }
        try {
          String dummyDirPath = new File(dummy_dir).getCanonicalPath();
          String dirPath = new File(data_dir).getCanonicalPath();
          if (dummyDirPath.equals(dirPath)) {
            throw new StorageInitializationException(
                String.format(
                    "%s can't be used as dummy dir and data dir at same time.", dummy_dir));
          }
          this.dataDir = data_dir;
          createDir(data_dir);
        } catch (IOException e) {
          throw new StorageInitializationException(
              String.format(
                  "Error reading dummy dir path %s and dir path %s: %s", dummy_dir, data_dir, e));
        }
      }
    } else {

      if (data_dir == null || data_dir.isEmpty()) {
        throw new StorageInitializationException("Dir not provided in non-dummy storage.");
      }
      File dataFile = new File(data_dir);
      if (dataFile.isFile()) {
        throw new StorageInitializationException(
            String.format("Data dir %s is not a directory.", data_dir));
      }
      this.dataDir = data_dir;
      createDir(data_dir);
    }
  }

  private void createDir(String dirPath) throws StorageInitializationException {
    Path path = Paths.get(dirPath);
    try {
      if (!Files.exists(path)) {
        Files.createDirectories(path);
      }
    } catch (IOException e) {
      throw new StorageInitializationException(
          String.format("Create directory %s error: " + e, dirPath));
    }
  }

  private void recoverFromDisk() {
    File file = new File(dataDir);
    File[] duDirs = file.listFiles();
    if (duDirs != null) {
      for (File duDir : duDirs) {
        if (duDir.isFile()) continue;
        try {
          logger.info("recovering {} from disk", duDir);
          Manager manager = new DataManager(duDir.toPath());
          managers.putIfAbsent(duDir.getName(), manager);
        } catch (IOException e) {
          logger.error("fail to recovery {} from disk ", duDir, e);
        }
      }
    }
  }

  private Manager getOrCreateManager(String storageUnit) throws IOException {
    if (!managers.containsKey(storageUnit)) {
      Manager manager = new DataManager(Paths.get(dataDir, storageUnit));
      managers.putIfAbsent(storageUnit, manager);
    }
    return managers.get(storageUnit);
  }

  @Override
  public TaskExecuteResult executeProjectTask(
      List<String> paths,
      TagFilter tagFilter,
      Filter filter,
      String storageUnit,
      boolean isDummyStorageUnit) {
    try {
      Manager manager;
      if (isDummyStorageUnit) {
        manager = dummyManager;
      } else {
        manager = getOrCreateManager(storageUnit);
      }
      RowStream rowStream = manager.project(paths, tagFilter, filter);
      rowStream = new ClearEmptyRowStreamWrapper(rowStream);
      rowStream = new FilterRowStreamWrapper(rowStream, filter);
      return new TaskExecuteResult(rowStream, null);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(null, e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit) {
    try {
      Manager manager = getOrCreateManager(storageUnit);
      manager.insert(dataView);
      return new TaskExecuteResult(null, null);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(null, e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    try {
      Manager manager = getOrCreateManager(storageUnit);
      manager.delete(paths, keyRanges, tagFilter);
      return new TaskExecuteResult(null, null);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(null, e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    if (storageUnit.equals("*")) {
      List<Column> columns = new ArrayList<>();
      for (Manager manager : managers.values()) {
        columns.addAll(manager.getColumns());
      }
      columns.addAll(dummyManager.getColumns());
      return columns;
    } else {
      throw new RuntimeException("not implemented!");
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage() throws PhysicalException {
    List<String> paths = new ArrayList<>();
    long start = Long.MAX_VALUE, end = Long.MIN_VALUE;
    for (Manager manager : managers.values()) {
      for (Column column : manager.getColumns()) {
        paths.add(column.getPath());
      }
      KeyInterval interval = manager.getKeyInterval();
      if (interval.getStartKey() < start) {
        start = interval.getStartKey();
      }
      if (interval.getEndKey() > end) {
        end = interval.getEndKey();
      }
    }
    paths.sort(String::compareTo);
    if (paths.isEmpty()) {
      throw new PhysicalTaskExecuteFailureException("no data");
    }
    if (start == Long.MAX_VALUE || end == Long.MIN_VALUE) {
      throw new PhysicalTaskExecuteFailureException("time range error");
    }
    ColumnsInterval columnsInterval =
        new ColumnsInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
    KeyInterval keyInterval = new KeyInterval(start, end);
    return new Pair<>(columnsInterval, keyInterval);
  }

  @Override
  public void close() throws PhysicalException {
    logger.info("closing...");
    for (Manager manager : managers.values()) {
      try {
        manager.close();
      } catch (Throwable e) {
        logger.error("fail to close manager", e);
      }
    }
    managers.clear();
  }
}
