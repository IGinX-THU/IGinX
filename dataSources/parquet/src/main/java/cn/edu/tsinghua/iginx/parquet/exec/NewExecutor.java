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
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;
import cn.edu.tsinghua.iginx.parquet.tools.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewExecutor implements Executor {

  private static final Logger logger = LoggerFactory.getLogger(NewExecutor.class);

  public String dataDir;

  private final Map<String, Manager> managers = new ConcurrentHashMap<>();

  private final Manager dummyManager;

  public NewExecutor(boolean hasData, boolean readOnly, String dataDir, String dummyDir)
      throws StorageInitializationException {
    this(hasData, readOnly, dataDir, dummyDir, null);
  }

  public NewExecutor(
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

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    close();
                  } catch (PhysicalException e) {
                    logger.error("Fail to close parquet executor, details: {}", e.getMessage());
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
                  "Error reading dummy dir path %s and dir path %s: " + e.getMessage(),
                  dummy_dir,
                  data_dir));
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

  private void createDir(String dirPath) {
    Path path = Paths.get(dirPath);
    try {
      if (!Files.exists(path)) {
        Files.createDirectory(path);
      }
    } catch (IOException e) {
      logger.error(String.format("Create directory %s error: " + e.getMessage(), dirPath));
    }
  }

  private void recoverFromDisk() throws PhysicalException {
    File file = new File(dataDir);
    File[] duDirs = file.listFiles();
    if (duDirs != null) {
      for (File duDir : duDirs) {
        if (duDir.isFile()) continue;
        try {
          Manager manager = new DataManager(duDir.toPath());
          managers.putIfAbsent(duDir.getName(), manager);
        } catch (IOException e) {
          logger.error("Fail to recover from disk ", e);
          throw new PhysicalException(e);
        }
      }
    }
  }

  private Manager getOrCreateManager(String storageUnit) throws PhysicalException, IOException {
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

    Manager manager;
    try {
      if (isDummyStorageUnit) {
        manager = dummyManager;
      } else {
        manager = getOrCreateManager(storageUnit);
      }
    } catch (Exception e) {
      logger.error("Fail to get du manager ", e);
      return new TaskExecuteResult(null, new PhysicalException("Fail to get du manager ", e));
    }

    try {
      RowStream rowStream = manager.project(paths, tagFilter, filter);
      rowStream = new ClearEmptyRowStreamWrapper(rowStream);
      rowStream = new FilterRowStreamWrapper(rowStream, filter);
      return new TaskExecuteResult(rowStream, null);
    } catch (PhysicalException e) {
      logger.error("Fail to project data ", e);
      return new TaskExecuteResult(null, new PhysicalException("Fail to project data ", e));
    }
  }

  @Override
  public TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit) {
    Manager manager;
    try {
      manager = getOrCreateManager(storageUnit);
    } catch (Exception e) {
      return new TaskExecuteResult(null, new PhysicalException("Fail to get du manager ", e));
    }

    try {
      manager.insert(dataView);
    } catch (Exception e) {
      logger.error("Fail to insert data ", e);
      return new TaskExecuteResult(null, new PhysicalException("Fail to insert data ", e));
    }

    return new TaskExecuteResult(null, null);
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    Manager manager;
    try {
      manager = getOrCreateManager(storageUnit);
    } catch (Exception e) {
      logger.error("Fail to get du manager ", e);
      return new TaskExecuteResult(null, new PhysicalException("Fail to get du manager ", e));
    }

    try {
      manager.delete(paths, keyRanges, tagFilter);
    } catch (Exception e) {
      logger.error("Fail to delete data ", e);
      return new TaskExecuteResult(null, new PhysicalException("Fail to delete data " + e));
    }

    return new TaskExecuteResult(null, null);
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    try {
      if (storageUnit.equals("*")) {
        recoverFromDisk();
        Map<String, Column> columns = new HashMap<>();
        for (Manager manager : managers.values()) {
          for (Column column : manager.getColumns()) {
            columns.put(column.getPhysicalPath(), column);
          }
        }
        for (Column column : dummyManager.getColumns()) {
          columns.put(column.getPhysicalPath(), column);
        }
        return new ArrayList<>(columns.values());
      } else {
        throw new RuntimeException("not implemented!");
      }
    } catch (Exception e) {
      logger.error("Fail to get columns ", e);
      throw new PhysicalException("fail to get columns ", e);
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage() throws PhysicalException {
    List<String> paths = new ArrayList<>();
    long start = Long.MAX_VALUE, end = Long.MIN_VALUE;
    for (Manager manager : managers.values()) {
      try {
        for (Column column : manager.getColumns()) {
          paths.add(column.getPath());
        }
      } catch (Exception e) {
        throw new PhysicalException("Fail to get paths", e);
      }
      try {
        KeyInterval interval = manager.getKeyInterval();
        if (interval.getStartKey() < start) {
          start = interval.getStartKey();
        }
        if (interval.getEndKey() > end) {
          end = interval.getEndKey();
        }
      } catch (Exception e) {
        logger.error("Fail to get key interval", e);
        throw new PhysicalException("fail to get key interval", e);
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
      } catch (Exception e) {
        throw new PhysicalException("failed to close local executor", e);
      }
      managers.clear();
    }
  }
}
