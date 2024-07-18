/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import cn.edu.tsinghua.iginx.parquet.manager.data.DataManager;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.DummyManager;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.EmptyManager;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.exception.IsClosedException;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class LocalExecutor implements Executor {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutor.class);

  public Path dataDir;

  private FileLock fileLock;

  public Path dummyDir;

  private final ConcurrentHashMap<String, Manager> managers = new ConcurrentHashMap<>();

  private final Manager dummyManager;

  private final Shared shared;

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public LocalExecutor(
      Shared shared,
      boolean hasData,
      boolean readOnly,
      String dataDir,
      String dummyDir,
      String dirPrefix)
      throws StorageInitializationException {
    this.shared = shared;

    testValidAndInit(hasData, readOnly, dataDir, dummyDir);

    if (this.dummyDir != null) {
      String embeddedPrefix;
      if (dirPrefix == null || dirPrefix.isEmpty()) {
        embeddedPrefix = Paths.get(dataDir).getFileName().toString();
      } else {
        embeddedPrefix = dirPrefix;
      }
      dummyManager = new DummyManager(Paths.get(dummyDir), embeddedPrefix);
    } else {
      dummyManager = new EmptyManager();
    }

    if (this.dataDir != null) {
      recoverFromDisk();
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    close();
                  } catch (Throwable e) {
                    LOGGER.error("fail to close parquet executor", e);
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
      this.dummyDir = Paths.get(dummy_dir);
    }

    if (!(has_data && read_only)) {
      if (data_dir == null || data_dir.isEmpty()) {
        throw new StorageInitializationException("Dir not provided in non-dummy storage.");
      }
      File dataFile = new File(data_dir);
      if (dataFile.isFile()) {
        throw new StorageInitializationException(
            String.format("Data dir %s is not a directory.", data_dir));
      }
      this.dataDir = Paths.get(data_dir);
      createDir(data_dir);
      this.fileLock = lockFile(dataDir, Constants.LOCK_FILE_NAME);
    }

    if (has_data && !read_only) {
      try {
        String dummyDirPath = new File(dummy_dir).getCanonicalPath();
        String dirPath = new File(data_dir).getCanonicalPath();
        if (dummyDirPath.equals(dirPath)) {
          throw new StorageInitializationException(
              String.format("%s can't be used as dummy dir and data dir at same time.", dummy_dir));
        }
      } catch (IOException e) {
        throw new StorageInitializationException(
            String.format(
                "Error reading dummy dir path %s and dir path %s: %s", dummy_dir, data_dir, e));
      }
    }
  }

  private FileLock lockFile(Path dataDir, String lockFileName)
      throws StorageInitializationException {
    Path path = dataDir.resolve(lockFileName);
    try {
      if (!Files.exists(path)) {
        Files.createDirectories(path.getParent());
        Files.createFile(path);
      }
      FileChannel channel = FileChannel.open(path, StandardOpenOption.APPEND);
      FileLock lock = channel.tryLock();
      if (lock == null) {
        throw new StorageInitializationException(
            String.format("lock file %s error: another program holds an overlapping lock", path));
      }
      return lock;
    } catch (OverlappingFileLockException e) {
      throw new StorageInitializationException(
          String.format("lock file %s error: this jvm holds an overlapping lock", path));
    } catch (IOException e) {
      throw new StorageInitializationException(String.format("lock file %s error: " + e, path));
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

  private void recoverFromDisk() throws StorageInitializationException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir)) {
      for (Path path : stream) {
        if (!Files.isDirectory(path)) continue;
        try {
          LOGGER.info("recovering {} from disk", path);
          getOrCreateManager(path.getFileName().toString());
        } catch (PhysicalException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      throw new StorageInitializationException(
          String.format("Error reading data dir %s: %s", dataDir, e));
    }
  }

  private Manager getOrCreateManager(String storageUnit) throws PhysicalException {
    if (dataDir == null) {
      throw new PhysicalException("data dir not provided");
    }
    if (storageUnit.startsWith("dummy")) {
      throw new PhysicalException("dummy storage unit not allowed");
    }

    return managers.computeIfAbsent(
        storageUnit,
        s -> {
          if (isClosed.get()) {
            throw new IsClosedException("executor is closed: " + dataDir);
          }
          try {
            return new DataManager(shared, dataDir.resolve(s));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public TaskExecuteResult executeProjectTask(
      List<String> paths,
      @Nullable TagFilter tagFilter,
      @Nullable Filter filter,
      @Nullable List<FunctionCall> calls,
      String storageUnit,
      boolean isDummyStorageUnit) {
    if (calls != null) {
      Preconditions.checkArgument(!calls.isEmpty(), "calls should not be empty");
      Preconditions.checkArgument(filter == null, "filter should be null");
      Preconditions.checkArgument(!isDummyStorageUnit, "dummy storage unit not allowed");
      return executeAggregationTask(paths, tagFilter, calls, storageUnit);
    } else {
      Preconditions.checkNotNull(filter, "filter should not be null");
      return executeProjectTask(paths, tagFilter, filter, storageUnit, isDummyStorageUnit);
    }
  }

  private TaskExecuteResult executeProjectTask(
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
      return new TaskExecuteResult(rowStream);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }
  }

  private TaskExecuteResult executeAggregationTask(
      List<String> patterns, TagFilter tagFilter, List<FunctionCall> calls, String storageUnit) {
    try {
      Manager manager = getOrCreateManager(storageUnit);
      RowStream rowStream = ((DataManager) manager).aggregation(patterns, tagFilter, calls);
      return new TaskExecuteResult(rowStream);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }
  }

  @Override
  public TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit) {
    try {
      Manager manager = getOrCreateManager(storageUnit);
      manager.insert(dataView);
      return new TaskExecuteResult();
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    try {
      Manager manager = getOrCreateManager(storageUnit);
      manager.delete(paths, keyRanges, tagFilter);
      return new TaskExecuteResult();
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(
      String storageUnit, Set<String> patterns, TagFilter tagFilter) throws PhysicalException {
    List<String> patternList = new ArrayList<>(patterns);
    if (patternList.isEmpty()) {
      return Collections.emptyList();
    }
    if (storageUnit.equals("*")) {
      List<Column> columns = new ArrayList<>();
      for (Manager manager :
          Iterables.concat(managers.values(), Collections.singleton(dummyManager))) {
        columns.addAll(manager.getColumns(patternList, tagFilter));
      }
      return columns;
    } else {
      throw new RuntimeException("not implemented!");
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    List<String> paths =
        dummyManager.getColumns(Collections.singletonList("*"), null).stream()
            .map(Column::getPath)
            .collect(Collectors.toList());
    if (dataPrefix != null) {
      paths =
          paths.stream().filter(path -> path.startsWith(dataPrefix)).collect(Collectors.toList());
    }
    paths.sort(String::compareTo);
    if (paths.isEmpty()) {
      throw new PhysicalTaskExecuteFailureException("no data");
    }
    ColumnsInterval columnsInterval =
        new ColumnsInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
    return new Pair<>(columnsInterval, KeyInterval.getDefaultKeyInterval());
  }

  @Override
  public void close() throws PhysicalException {
    LOGGER.info("closing...");

    try {
      dummyManager.close();
    } catch (Throwable e) {
      LOGGER.error("fail to close dummy manager", e);
    }

    isClosed.set(true);

    if (fileLock != null) {
      try {
        fileLock.release();
        fileLock.channel().close();
        fileLock = null;
      } catch (IOException e) {
        LOGGER.error("fail to release lock file", e);
      }
    }

    managers.forEach(
        1,
        (unit, manager) -> {
          MDC.put(Constants.STORAGE_UNIT_NAME, unit);
          try {
            manager.close();
          } catch (Throwable e) {
            LOGGER.error("fail to close manager", e);
          } finally {
            MDC.remove(Constants.STORAGE_UNIT_NAME);
          }
        });
  }
}
